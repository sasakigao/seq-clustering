package com.sasaki.train

import scala.collection.parallel.immutable._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler

import com.sasaki.utils._
import com.sasaki.cluster._


/**
 * Stage 4 -- Cluster high-dim vectors using dimensionally reduction. 
 */
object ReducedClu {
	val appName = "Dimred clustering"

	val delimiter = ","
	// Number of motions each level
	val numTop = 5
	// 5 seems better
	val k = 5
	val numIterations = 20
	val initMode = "k-means||"
	val seed = 22L
	// number of dimensions remained each level
	val numDimKept = 5


	def main(args: Array[String]) = {
		val basePath = args(0)
		val model = args(1)
		require(model == "km" || model == "bkm", s"Illegal model ${model}")
		val modelK = args(2).toInt
		require(modelK > 0, s"Number of clusters must be positive but got ${modelK}")
		// Writing path
		val redEventsHDFS = s"${basePath}/events"
		val redVecsHDFS = s"${basePath}/vecs"

	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.scheduler.mode", "FAIR")    // important for multiuser parallelization
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[LabeledSet], classOf[NewLabeledPoint], classOf[VecPack]))
  		val sc = new SparkContext(confSpark)

		/**
		 * Data files should be like "motion, score, motion, score, label"
		 */
		val rawData: RDD[Array[String]] = sc.textFile(basePath)
			.map(_.split(delimiter))
		rawData.persist()
		val rowLen = rawData.first.size
		val numGroups = (rowLen - 1) / (2 * numTop)

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
				groupData.unpersist()
				(g -> (alignedData, union))
			}
		LogHelper.log(aligned.map(_._2._2.size).sum, s"total union size ")

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
				LogHelper.log(score, s"score of group $g column $motion")
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

		/**
		 * Join each group to restore the raw order.
		 * (ID+label, vector)
		 */
		val assembled: RDD[NewLabeledPoint] = reducedData.reduce{ (rdd1, rdd2) =>
			val joined = rdd1.join(rdd2)                   // TAKE TIME
			joined.mapValues(x => x._1 ++ x._2)
		}.mapPartitions(_.map(x => new NewLabeledPoint(x._1.toDouble, Vectors.dense(x._2))))
		assembled.persist()

		// 3rd saved vectors -- dimreduced vectors
		assembled.repartition(1).saveAsTextFile(redVecsHDFS)
		
		TrainEva.input(assembled, basePath, model, modelK)

		sc.stop()
	}
}