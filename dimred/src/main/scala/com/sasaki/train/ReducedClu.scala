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
 * Cluster high-dim vectors using dimensionally reduction. 
 */
object ReducedClu {
	val appName = "Clustering of dim-reduced grade vecs"

    	// Raw data files to read
	val dataHDFS = "hdfs:///netease/ver2/seq2vec/916new/lev40-top5-bg2"

	val delimiter = ","

	// Params for column clustering
	val numTop = 5
	val k = 5
	val numIterations = 20
	val numDimKept = 5           // number of dimensions remained each level
	val initMode = "k-means||"
	val seed = 22L


	def main(args: Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	// confSpark.registerKryoClasses(Array(classOf[Proximity]))
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
		// val targetBefore = rawData.filter(_.last == "26123804150").collect.head.toList
		// LogHelper.log(targetBefore, "target before")

		/**
		 * Group step. Separate the bulk data into parallel groups.
		 * Each line takes a label.
		 */
		val parData: ParSeq[(Int, RDD[Array[String]])] = (0 until numGroups).par.map{ g =>
			val startAt = g * numTop * 2
			val endAt = (g + 1) * numTop * 2
			val groupData = rawData.map{ x =>
				val labelStr = x.last
				x.slice(startAt, endAt) :+ labelStr
			}
			// Adjust numPartitons of group data
			// groupData.coalesce(groupData.getNumPartitions * 2 / numGroups, false)
			(g, groupData)
		}
		rawData.unpersist()
		// parData.foreach{ case (g, data) =>
		// 	data.persist()
		// 	LogHelper.log(s"Group $g counts ${data.count}.")
		// 	LogHelper.log(data.first.toList, s"sample of $g")
		// 	data.unpersist()
		// }

		/**
		 * Union step. Keep frepuency and duration.
		 */
		val aligned: ParSeq[(Int, (RDD[LabeledPoint], Int))] = 
			parData.map{ case (g, groupData) =>
				groupData.persist()
				// Select a identifier
				val union: Array[String] = groupData.flatMap{ row =>
					(0 until numTop).map(x => row(x * 2))}
					.distinct.collect
				// LogHelper.log(union.size, s"union size of group $g")
				// LogHelper.log(union.toList, s"union of group $g")
				val unionBc = sc.broadcast(union)
				val alignedData = groupData.mapPartitions{ iter =>
					val rounds = (0 until numTop)
					val criterion = unionBc.value
					iter.map{ vec =>
						val eventMap = rounds.map( x => 
							(vec(2 * x), vec.slice(2 * x + 1, 2 * x + 2)))
							.toMap
						val alignedVec = criterion.map(eventMap.getOrElse(_, Array("0")))
							.flatMap(_.map(_.toDouble))
						val label = vec.last.toDouble
						LabeledPoint(label, Vectors.dense(alignedVec))
					}
				}.persist()
				// LogHelper.log(alignedData.first.features.toArray.toList, s"aligned features of group $g")
				// LogHelper.log(alignedData.first.label, s"aligned label of group $g")
				groupData.unpersist()
				(g -> (alignedData, union.size))
			}

		/**
		 * Column clustering step. 
		 * Group-level is serial while column-level is parallel.
		 */
		val cohesions: Map[Int, Array[(Int, Double)]] = aligned.seq.map{ case (g, (gData, numCols)) => 
			val colResults = (0 until numCols).par.map{ col =>
				val colData: RDD[Double] = gData.mapPartitions(iter => iter.map(_.features.toArray(col)))
				val ct = new ColumnTrainer(colData)        // TAKE TIME
				val score = ct.kmeans(k, numIterations, initMode, seed)
				LogHelper.log(score, s"score of group $g column $col")
				(col, score)
			}
			(g, colResults.toArray)
		}.toMap

		/**
		 * Dimred step.
		 * 2 way. Reducing by a ratio or just filter the cols with 0.
		 * A 0 cohesion means this dimension has 2 or less different values.
		 */
		val numLeft: Map[Int, Array[Int]] = cohesions.mapValues{ gRes: Array[(Int, Double)] => 
			val unionLen = gRes.size
			// LogHelper.log(unionLen.size, "unionLen")
			val resonable = gRes.filter(_._2 != 0).slice(0, numDimKept)
			// LogHelper.log(resonable.size, "0-removed unionLen")
			resonable.slice(0, numDimKept).map(_._1)
		}

		// Reduce dimensions and recover group order.
		val reducedData: Array[RDD[(Double, Array[Double])]] = aligned.map{ case (g, (gData, _)) => 
			val keptCols = numLeft(g)
			val reduced = gData.mapPartitions{ iter =>
				iter.map{ lp =>
					val arr = keptCols.map(x => lp.features.toArray(x))
					val diff = numDimKept - keptCols.size
					if (diff == 0) {
						// Not LabeledPoint for the join later
						(lp.label, arr)
					} else {
						(lp.label, arr ++ Array(0, diff - 1).map(_ => 0.0))
					}
				}
			}
			(g, reduced)
		}.toArray.sortBy(_._1).map(_._2)

		// reducedData.foreach{ rdd =>
			// val rddSample = rdd.first
			// LogHelper.log(rddSample._1, "reducedData label")
			// LogHelper.log(rddSample._2.toList, "reducedData vector")
		// }

		/**
		 * Join each group to restore the raw order.
		 */
		val assembled: RDD[LabeledPoint] = reducedData.reduce{ (rdd1, rdd2) =>
			val joined = rdd1.join(rdd2)                   // TAKE TIME
			joined.mapValues(x => x._1 ++ x._2)
		}.mapPartitions(_.map(x => LabeledPoint(x._1, Vectors.dense(x._2))))
		assembled.persist()

		// val targetAfter = assembled.filter(_.label.toInt.toString == "26123804150").collect.head
		// val targetAfter = assembled.first
		// LogHelper.log(targetAfter.label, "target after label")
		// LogHelper.log(targetAfter.features.toArray.toList, "target after vector")

		TrainEva.input(assembled)

		sc.stop()
	}
}