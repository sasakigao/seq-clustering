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

    	// Raw data files to read
	val levelDataHDFS = "hdfs:///netease/ver2/gen/916/level/"
    	// Bad guys lookup file to read
	val badguysLookupFile = "hdfs:///netease/ver2/datum/bg-main"

	// Write to
	val resPath = "hdfs:///netease/ver2/seq2vec/916new/"

	// Event number in each level
	val k = 5

	/**
	 * Give a sample number
	 */
	def main(args : Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  		val sc = new SparkContext(confSpark)
  		val sqlContext = new SQLContext(sc)

  		val badguys = sc.textFile(badguysLookupFile, 1).collect.toSet

  		val schema = StructType(
    			StructField("role", StringType, nullable = false) ::
    			StructField("mark", StringType, nullable = false) ::
			StructField("seqs", ArrayType(StringType), nullable = false) :: 
			Nil)

  		// id -> (grade, motion)
		val levelsData = sqlContext.read.schema(schema).json(levelDataHDFS).rdd
			.map{row => 
				val motion = row.getSeq[String](2).map(_.split("@")(1))
				(row.getString(0), (row.getString(1).toInt, motion))}
			.persist()
		val roleReduced = levelsData.combineByKey(
			(v: (Int, Seq[String])) => Seq(v),
			(c: Seq[(Int, Seq[String])], v: (Int, Seq[String])) => c :+ v,
			(c1: Seq[(Int, Seq[String])], c2: Seq[(Int, Seq[String])]) => c1 ++ c2)
		
		// Filtered valid-span users
		val spans: RDD[(String, Seq[(Int, Seq[String])])] = roleReduced.filter{ seq =>
			val levelSeq = seq._2.map(_._1).toSet
			(2 to 40).forall(levelSeq contains _)
		}.mapPartitions{ iter =>
			iter.map{ role =>
				val range = role._2.filter(_._1 <= 40)
				(role._1, range)
			}
		}

		// Count by level
		import scala.collection.mutable.{Map => muMap}
		val eventCounts: RDD[(String, Seq[(Int, muMap[String, Int])])] = spans.mapPartitions{ iter =>
			iter.map{ role =>
				val levels = role._2
				val countLevels = levels.map{ level =>
					val counts = level._2.groupBy(x=>x).mapValues(_.size)
					(level._1, muMap(counts.toSeq: _*))
				}
				(role._1, countLevels)
			}
		}

		// Compute maximin of each level each motion.
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
			.mapValues(x => x.map(y => (y._1, (y._2.max, y._2.min))))
			.collectAsMap

		// Scale motion frequency by level to (0,100)
		val scaledCounts: RDD[(String, Seq[(Int, muMap[String, Double])])] = eventCounts.mapPartitions{ iter =>
			iter.map{ roleStat =>
				val statSeq = roleStat._2
				val scaledRole = statSeq.map{ levelStat =>
					val level = levelStat._1
					val scaledStat = levelStat._2.map{ case (motion, count) => 
						val (max, min) = maximin(level)(motion)
						val scaled = if (max == min) 1 else 1.0 * (count - min) / (max - min)
						(motion, 100 * scaled)
					}
					(levelStat._1, scaledStat)
				}
				(roleStat._1, scaledRole)
			}
		}

		// Select the topk columns
		val topkResult = scaledCounts.mapPartitions{ iter =>
			iter.map{ role =>
				val seq = role._2
				val seqSlice = seq.sortBy(_._1).flatMap{ level =>
					val ordered = level._2.toSeq.sortBy(-_._2)
					val len = ordered.size
					if (len >= k) 
						ordered.slice(0, k)
					else 
						ordered ++ Array.range(0, k - len).map(_ => ("0", 0.0))
				}
				val label = if (badguys contains role._1) 1 else 0
				seqSlice :+ (role._1 + label)
			}
		}

		topkResult.repartition(1).saveAsTextFile(resPath)

		sc.stop()
  	}

}
