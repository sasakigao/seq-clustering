package com.sasaki.reduction

import scala.collection.parallel.immutable._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint

import java.text.SimpleDateFormat
import java.util.Date

import com.sasaki.utils._
import com.sasaki.trainer._


/**
 * Stage 4 -- Dim reduction
 */
object Dimred {
	val appName = "Dimred"

	val delimiter = ","
	// Number of motions each level
	val numTop = 5
	// 5 seems better
	// val k = 5
	val numIterations = 20
	val initMode = "k-means||"
	val seed = 22L
	// number of dimensions remained each level
	// val numDimKept = 5

    	// Reading path
    	// val isExpanded = false
	// val resPathSuffix = if (isExpanded) "expanded" else "raw"
    	// val basic = s"hdfs:///netease/ver2/seq2vec/916main/2to40/k5/${resPathSuffix}/unaligned"
    	// val fileName = "part-00000"
	// val dataHDFS = s"${basic}/${fileName}"
	// Writing path
	// val redEventsHDFS = s"${basic}/redevents"
	// val redVecsHDFS = s"${basic}/redvecs"

	/**
	 * @param basePath -- "hdfs:///netease/ver2/seq2vec/916main/2to40/k5/raw/unaligned"
	 * @param k -- Int number that ColumnTrainer takes for clustering
	 * @param numDimKept -- Number of left dimensions each level
	 */
	def main(args: Array[String]) = {
		val basePath = args(0)
		val k = args(1).toInt
		require(k > 0, s"Number of clusters must be positive but got ${k}")
		val numDimKept = args(2).toInt
		require(numDimKept > 0, s"Number of kept dims must be positive but got ${numDimKept}")

		val dataHDFS = s"${basePath}/part-00000"
		val dateStr = getTime
		val redEventsHDFS = s"${basePath}/${dateStr}/redevents/"
		val redVecsHDFS = s"${basePath}/${dateStr}/redvecs/"

	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.scheduler.mode", "FAIR")    // important for multiuser parallelization
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[NewLabeledPoint], classOf[VecPack]))
  		val sc = new SparkContext(confSpark)

		/**
		 * Data files should be like "motion, score, motion, score, label"
		 */
		val rawData: RDD[Array[String]] = sc.textFile(dataHDFS)
			.map(_.split(delimiter))
		rawData.persist()
		val rowLen = rawData.first.size
		val numGroups = (rowLen - 1) / (2 * numTop)
		// LogHelper.log(rowLen, "rowLen")
		// LogHelper.log(numGroups, "numGroups")

		/**
		 * Group step. Separate the bulk data into parallel groups.
		 * Each group of a line takes a label.
		 */
		val parData: ParSeq[(Int, RDD[VecPack])] = (0 until numGroups).par.map{ g =>
			val startAt = g * numTop * 2
			val endAt = (g + 1) * numTop * 2
			val groupData = rawData.map{ line =>
				val feature = line.slice(startAt, endAt)
				val feaMap = feature.grouped(2).map(x => (x.head, x.last.toDouble)).toMap
				VecPack(line.last, feaMap)
			}
			// Adjust numPartitons of group data
			// groupData.coalesce(groupData.getNumPartitions * 2 / numGroups, false)
			(g, groupData)
		}
		rawData.unpersist()
		// parData.foreach{ case (g, data) =>
		// 	data.persist()
		// 	LogHelper.log(s"Group $g counts ${data.count}.")
		// 	LogHelper.log(data.first, s"sample of $g")
		// 	data.unpersist()
		// }

		/**
		 * Union step. (GroupId, (RDD, union))
		 * Group-level is parallel.
		 */
		val aligned: ParSeq[(Int, (RDD[VecPack], Array[String]))] = 
			parData.map{ case (g, groupData) =>
				groupData.persist()
				// Select a identifier
				val union: Array[String] = groupData.flatMap(_.feaMap.keySet)
					.distinct.collect
				// LogHelper.log(union.size, s"union size of group $g")
				// LogHelper.log(union.toList, s"union of group $g")
				val unionBc = sc.broadcast(union)
				val alignedData = groupData.mapPartitions{ iter =>
					val rounds = (0 until numTop)
					val criterion = unionBc.value
					iter.map{ vp =>
						val eventMap = vp.feaMap
						// Aligned row takes tuple (motionId, value)
						val alignedMap = criterion.map(x => (x, eventMap.getOrElse(x, 0.0))).toMap
						vp.setFeaMap(alignedMap)
						vp
					}
				}.persist()
				// LogHelper.log(alignedData.first, s"aligned features of group $g")
				groupData.unpersist()
				(g -> (alignedData, union))
			}
		// LogHelper.log(aligned.map(_._2._2.size).sum, s"total union size ")

		/**
		 * Column clustering step. GroupID -> Arr(motion, score)
		 * Group-level is serial while column-level is parallel.
		 */
		val cohesions: Map[Int, Array[(String, Double)]] = aligned.seq.map{ case (g, (gData, union)) => 
			val gd = gData.map(_.feaMap)
			val colResults = union.par.map{ motion =>
				val colData: RDD[Double] = gd.mapPartitions(iter => iter.map(_(motion)))
				val ct = new ColumnTrainer(colData)        // TAKE TIME
				val score = ct.kmeans(k, numIterations, initMode, seed)
				// LogHelper.log(score, s"score of group $g column $motion")
				(motion, score)
			}
			(g, colResults.toArray)
		}.toMap

		/**
		 * Dimred step.
		 * 2 way. Reducing by a ratio or just filter the cols with 0.
		 * A 0 cohesion means this dimension has 2 or less different values.
		 */
		val numLeft: Map[Int, Array[(String, Double)]] = cohesions.mapValues{ gRes => 
			val resonable = gRes.filter(_._2 != 0)
			// LogHelper.log(resonable.toList, s"resonable")
			resonable.slice(0, numDimKept)
		}

		// 2nd saved vectors -- dimreduced motion id
		sc.parallelize(numLeft.mapValues(_.toList).toList, 1).saveAsTextFile(redEventsHDFS)

		// Reduce dimensions and recover group order.
		val reducedData: Array[RDD[(String, Array[Double])]] = aligned.map{ case (g, (gData, _)) => 
			val keptCols = numLeft(g).map(_._1)
			val reduced = gData.mapPartitions{ iter =>
				iter.map{ feat =>
					val arr = keptCols.map(x => feat.feaMap(x))
					val diff = numDimKept - keptCols.size
					(feat.label, arr ++ Array.fill(diff)(0.0))
				}
			}
			(g, reduced)
		}.toArray.sortBy(_._1).map(_._2)

		// reducedData.foreach{ rdd =>
		// 	val rddSample = rdd.first
		// 	LogHelper.log(rddSample._1, "reducedData label")
		// 	LogHelper.log(rddSample._2.toList, "reducedData vector")
		// }

		/**
		 * Join each group to restore the raw order.
		 * (ID+label, vector)
		 */
		val assembled: RDD[NewLabeledPoint] = reducedData.reduce{ (rdd1, rdd2) =>
			val joined = rdd1.join(rdd2)                   // TAKE TIME
			joined.mapValues(x => x._1 ++ x._2)
		}.mapPartitions(_.map(x => new NewLabeledPoint(x._1.toDouble, Vectors.dense(x._2))))
		assembled.persist()

		// val targetAfter = assembled.first
		// LogHelper.log(targetAfter.label, "target after label")
		// LogHelper.log(targetAfter.features.toArray.toList, "target after vector")

		// 3rd saved vectors -- dimreduced vectors
		assembled.repartition(1).saveAsTextFile(redVecsHDFS)
		
		sc.stop()
	}

	def getTime = {
  		val dt = new Date()
     		val sdf = new SimpleDateFormat("MMdd-HHmm")
     		sdf.format(dt)
  	}
}