package com.sasaki.train

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel, BisectingKMeans, BisectingKMeansModel}

import com.sasaki.utils._


object TrainEva {
	val evaResultPath = "hdfs:///netease/ver2/dimred/eva"

	def input(data: RDD[LabeledPoint]) = {
		val predAndLabel = km(data, 4, 100, 21L)
		val res = wrap(predAndLabel)
		LogHelper.log(res.collectAsMap, "res")
		// res.repartition(1).saveAsTextFile(evaResultPath)
	}

	// algo is "km" or "bkm"
	def km(data: RDD[LabeledPoint], k: Int, numIter: Int, seed: Long) = {
		val features = data.map(_.features).persist()
		val model = new KMeans().setK(k).setMaxIterations(numIter).setSeed(seed).run(features)
		val predAndLabel = data.mapPartitions{ iter =>
			iter.map{ lp =>
				val prediction = model.predict(lp.features)
				(prediction.toInt, lp.label.toLong.toString.last)
			}
		}
		features.unpersist()
		predAndLabel
	}

	def bkm(data: RDD[LabeledPoint], k: Int) = {
		val features = data.map(_.features).persist()
		val model = new BisectingKMeans().setK(4).run(features)
		val predAndLabel = data.mapPartitions{ iter =>
			iter.map{ lp =>
				val prediction = model.predict(lp.features)
				(prediction.toInt, lp.label.toLong.toString.last)
			}
		}
		features.unpersist()
		predAndLabel	
	}

	def wrap(predAndLabel: RDD[(Int, Char)]) = {
		predAndLabel.combineByKey(
				(v: Char) => Seq(v),
				(c: Seq[Char], v: Char) => c :+ v,
				(c1: Seq[Char], c2: Seq[Char]) => c1 ++ c2)
			.mapValues(_.groupBy(x => x)
			.mapValues(_.size))
	}

}