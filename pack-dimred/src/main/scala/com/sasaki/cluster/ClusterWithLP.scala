package com.sasaki.cluster

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering._

import com.sasaki.utils._


/**
 * Input LabeledPoint, output LabeledSet.
 */
class ClusterWithLP(val data: RDD[NewLabeledPoint]) {
	val features = data.map(_.features).persist()

	// KMeans algorithm.
	def km(k: Int, numIter: Int, seed: Long): 
			RDD[LabeledSet] = {
		val model = new KMeans().setK(k)
			.setMaxIterations(numIter)
			.setSeed(seed)
			.run(features)
		val predAndLP = data.mapPartitions{ iter =>
			iter.map{ lp =>
				val prediction = model.predict(lp.features).toInt
				val labelMix = lp.label.toLong
				val label = labelMix.%(10).toInt
				val id = labelMix / 10
				val vec = lp.features.toArray
				LabeledSet(vec, id, label, prediction)
			}
		}
		predAndLP
	}

	// Bisecting-KMeans algorithm
	def bkm(k: Int, numIter: Int, seed: Long, minDivSize: Double): 
			RDD[LabeledSet] = {
		val model = new BisectingKMeans().setK(k)
			.setMaxIterations(numIter)
			.setSeed(seed)
			.setMinDivisibleClusterSize(minDivSize)
			.run(features)
		val predAndLP = data.mapPartitions{ iter =>
			iter.map{ lp =>
				val prediction = model.predict(lp.features).toInt
				val labelMix = lp.label.toLong
				val label = labelMix.%(10).toInt
				val id = labelMix / 10
				val vec = lp.features.toArray
				LabeledSet(vec, id, label, prediction)
			}
		}
		predAndLP	
	}

	def releaseRDD = features.unpersist()

}