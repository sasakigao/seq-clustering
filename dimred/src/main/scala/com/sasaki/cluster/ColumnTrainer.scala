package com.sasaki.cluster

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}


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
		// val score = computeScore(model, features)
		val score = model.computeCost(features)
 		features.unpersist()
		score
 	}

 	// Cohesion & separation
 	def computeScore(model: KMeansModel, features: RDD[Vector]) = {
 		val cluCenters = model.clusterCenters
 		val cluCentersWithId = cluCenters.map(x => (model.predict(x), x.toArray)).toMap
 		val center = cluCenters.map(_.toArray).reduce{ (x, y) => 
 			x.zip(y).map(z => z._1 + z._2)
 		}.map(_ / cluCenters.size)

 		// id -> (its center, distance to center)
 		val predictions = features.mapPartitions{ iter =>
 			iter.map{ fea =>
	 			val id = model.predict(fea)
	 			val cluCenter = cluCentersWithId(id).toArray
	 			(id, (cluCenter, fea.toArray))
 			}
 		}
 		predictions.persist()
 		val sum = features.count
 		val idCounts = predictions.countByKey
 		// Separation
 		val SSB = idCounts.map{ case (id, count) => 
 			val vec = cluCentersWithId(id)
 			val weight = 1.0 * count / sum
 			val dist = vec.zip(center).map(x => (x._1 - x._2) * (x._1 - x._2)).sum
 			weight * dist
 		}.sum

 		// Cohesion
 		val SSE = predictions.mapPartitions{ iter =>
 			iter.map{ case (_, (cluCenter, fea)) =>
 				cluCenter.zip(fea).map(x => (x._1 - x._2) * (x._1 - x._2)).sum
 			}
 		}.sum
 		predictions.unpersist()
 		(SSE, SSB)
 	}
 }