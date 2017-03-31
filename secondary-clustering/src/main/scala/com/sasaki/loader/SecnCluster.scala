package com.sasaki.loader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import com.sasaki.utils._
import com.sasaki.cluster._


/**
 * Stage 5 -- Secondary clustering
 */
object SecnCluster {
	val appName = "Secondary clustering"

	val delimiter = ","

	/**
	 * @param basePath -- "hdfs:///netease/ver2/seq2vec/916main/2to40/k5/raw/unaligned"
	 * @param batchStr -- "3,10" means try from 3 clusters to 10
	 * @param model -- "km" or "bkm"
	 * @param rounds -- Running times of one model
	 * @param writePath -- save results
	 */
	def main(args: Array[String]) = {
		val basePath = args(0)
		val vecsHDFS = s"${basePath}/part-00000"
		val batchStr = args(1).split(",")
		require(batchStr.head.toInt > 0 && batchStr.last.toInt > 0, 
			s"Number of clusters must be valid int but got ${batchStr}")
		val batch = batchStr.head.toInt to batchStr.last.toInt
		val model = args(2)
		require(model == "km" || model == "bkm", s"Illegal model ${model}")
		val rounds = args(3)
		require(rounds.toInt > 0, s"Number of rounds must be valid int but got ${rounds}")
		val writePath = args(4)

	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.scheduler.mode", "FAIR")               // important for multiuser parallelization
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[LabeledSet], classOf[NewLabeledPoint]))
  		val sc = new SparkContext(confSpark)

		/**
		 * Data files should be like "id+label, features"
		 */
		val features: RDD[NewLabeledPoint] = sc.textFile(vecsHDFS)
			.mapPartitions{ iter =>
				iter.map{ line =>
					val fields = line.split(delimiter).map(_.toDouble)
					new NewLabeledPoint(fields.last, Vectors.dense(fields.init))
				}
			}
		
		TrainEva.input(features, batch, model, rounds.toInt, writePath)

		sc.stop()
	}

}