package com.sasaki.vector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types._

import com.sasaki.utils._


/**
 * Stage 3 -- Extract top-k motion info of 40 levels for dimred-clustering.
 * The frequency is replaced by the a scaling value from 0 to 100 by column.
 */
object Events {
	val appName = "Seq2vec"

	// Event number in each level
	val k = 5
	val levelRange = (2 to 40)

    	// Level data of step 1
	val levelDataHDFS = "hdfs:///netease/ver2/gen/916/level/"
	val badguysLookupFile = "hdfs:///netease/ver2/datum/bg-main"
	// Write to
	val isExpanded = false
	val resPathSuffix = if (isExpanded) "expanded" else "raw"
	// val resPath = s"hdfs:///netease/ver2/seq2vec/916main/${levelRange.head}to${levelRange.last}/k${k}/${resPathSuffix}/unaligned/"
	val resPath = "hdfs:///netease/ver2/compare/unnorm"


	def main(args : Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[CsvObj]))
  		val sc = new SparkContext(confSpark)
  		val sqlContext = new SQLContext(sc)

  		val badguys = sc.textFile(badguysLookupFile, 1).collect.toSet
  		val motions = sc.textFile("hdfs:///netease/ver2/datum/operations-user", 1).collect.map(_.split(",").head)

  		val schema = StructType(
    			StructField("role", StringType, nullable = false) ::
    			StructField("mark", StringType, nullable = false) ::
			StructField("seqs", ArrayType(StringType), nullable = false) :: 
			Nil)

		val levelsData = sqlContext.read.schema(schema).json(levelDataHDFS).rdd
			.map{ row => 
				val motionSer = row.getSeq[String](2).map( x =>
					if (isExpanded) MoreEvents.expand(x) else x.split("@")(1))
				(row.getString(0), (row.getString(1).toInt, motionSer))}
  		// id -> (L1,L2,L3...)
		val roleReduced = levelsData.combineByKey(
			(v: (Int, Seq[String])) => Seq(v),
			(c: Seq[(Int, Seq[String])], v: (Int, Seq[String])) => c :+ v,
			(c1: Seq[(Int, Seq[String])], c2: Seq[(Int, Seq[String])]) => c1 ++ c2)
		
		// Filter and trim users to fixed level span
		val spans: RDD[(String, Seq[(Int, Seq[String])])] = roleReduced.filter{ case (_, levSeq) =>
			val levels = levSeq.map(_._1).toSet
			levelRange.forall(levels contains _)
		}.mapPartitions{ iter =>
			iter.map{ role =>
				val span = role._2.filter(_._1 <= levelRange.last)
				(role._1, span)
			}
		}

		// Counter of each motion bu level
		import scala.collection.mutable.{Map => muMap}
		val eventCounts: RDD[(String, Seq[(Int, muMap[String, Int])])] = spans.mapPartitions{ iter =>
			iter.map{ case (id, seq) =>
				val countLevels = seq.map{ case (level, motions) =>
					val counts = motions.groupBy(x=>x).mapValues(_.size)
					(level, muMap(counts.toSeq: _*))
				}
				(id, countLevels)
			}
		}.persist()
		val numPlayer = eventCounts.count()

		// Compute maximin of each level of each motion.
		import scala.collection.{Map => sMap}
		val maximin: sMap[Int, muMap[String, (Int, Int)]] = eventCounts.flatMap(_._2)
			.combineByKey(
				(v: muMap[String, Int]) => v.map(x => (x._1, Seq(x._2))),
				(c: muMap[String, Seq[Int]], v: muMap[String, Int]) => {
					v.foreach{case (k, v) => if (c contains k) c(k) = c(k).:+(v) else c += (k -> Seq(v))}
					c
				},
				(c1: muMap[String, Seq[Int]], c2: muMap[String, Seq[Int]]) => {
					c2.foreach{case (k, v) => if (c1 contains k) c1(k) = c1(k) ++ c2(k) else c1 += (k -> v)}
					c1
				})
			.mapValues{ events => 
				events.map{ case (event, countSet) => 
					val max = countSet.max
					val min = if (countSet.size == numPlayer) countSet.min else 0
					(event, (max, min))
				}
			}.collectAsMap

		// Scale the frequency by level to (0,100)
		val scaledCounts: RDD[(String, Seq[(Int, muMap[String, Double])])] = eventCounts.mapPartitions{ iter =>
			iter.map{ roleStat =>
				val statSeq = roleStat._2
				val scaledRole = statSeq.map{ levelStat =>
					val level = levelStat._1
					val scaledStat = levelStat._2.map{ case (motion, count) => 
						val (max, min) = maximin(level)(motion)
						val scaled = if (max == min) 0 else 1.0 * (count - min) / (max - min)
						(motion, 100 * scaled)
					}
					(levelStat._1, scaledStat)
				}
				(roleStat._1, scaledRole)
			}
		}

		// No dim-reduction, so aligned
		// val topkResult = scaledCounts.mapPartitions{ iter =>
		// 	iter.map{ case (role, scaledCounter) => 
		// 		val seqSlice = scaledCounter.sortBy(_._1).flatMap{ level =>
		// 			val seqCurr = level._2
		// 			motions.map(x => seqCurr.getOrElse(x, 0.0))					
		// 		}
		// 		val label = if (badguys contains role) 1 else 0
		// 		new CsvObj2(seqSlice, role + label)
		// 	}
		// }

		// Select the top k columns, use 0 if cols less than k
		val topkResult = scaledCounts.mapPartitions{ iter =>
			iter.map{ case (role, scaledCounter) =>
				val seqSlice = scaledCounter.sortBy(_._1).flatMap{ level =>
					val ordered = level._2.toSeq.sortBy(-_._2)
					val len = ordered.size
					if (len >= k) 
						ordered.slice(0, k)
					else 
						ordered ++ Array.range(0, k - len).map(_ => ("0", 0.0))
				}
				val label = if (badguys contains role) 1 else 0
				new CsvObj(seqSlice, role + label)
			}
		}

		// 1st saved vectors -- fixed unaligned vectors with motion id
		topkResult.repartition(1).saveAsTextFile(s"${resPath}/events/")
		// topkResult.repartition(1).saveAsTextFile("hdfs:///netease/temp/a")

		sc.stop()
  	}
}
