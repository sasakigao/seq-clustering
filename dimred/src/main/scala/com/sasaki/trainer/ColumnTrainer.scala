package com.sasaki.trainer

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}


/**
 * Clustering or other model like entropy.
 */
 class ColumnTrainer(val data: RDD[Double]) {

 	def kmeans(k: Int, numIterations: Int, initMode: String, seed: Long): Double = {
 		val features = data.map(x => Vectors.dense(x.toDouble))
 		features.persist()
 		val model = new KMeans()
			.setK(k)
		      	.setMaxIterations(numIterations)
			.setInitializationMode(initMode)
			.setSeed(seed)
		      	.run(features)
		val score = model.computeCost(features)
 		features.unpersist()
		score
 	}

 }