package com.sasaki.cluster

import scala.collection.{Map => sMap}
import scala.collection.mutable.{Map => muMap}
import scala.collection.immutable.{Range, Map => imMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering._

import com.sasaki.utils._


/**
 * Use multi-k to cluster and select the cluster of best recall to cluster secondarily.
 * First is km or bkm, second is km only.
 */
object TrainEva {
	val evaResultPath = "hdfs:///netease/ver2/dimred/eva"

	val numIter = 100
	val seed = 21L
	val minDivSize = 0.01
	val subNumIter = 100
	val subSeed = 21L
	val subMinDivSize = 0.01

	def input(data: RDD[NewLabeledPoint], batch: Range) = {
		val algo1 = new ClusterWithLP(data)
		val numBad = data.filter(_.label % 10 == 1.0).count
		batch.par.foreach{ k =>
			Array("km", "bkm").foreach{ model =>
				val lsRDD1 = model match {
					case "km" => algo1.km(k, numIter, seed)
					case "bkm" => algo1.bkm(k, numIter, seed, minDivSize)
				} 
				lsRDD1.persist()
				val summary1 = wrapSummary(lsRDD1, numBad)
				val suspicious1 = wrapPositive(lsRDD1, summary1)

				val algo2 = new ClusterWithLP(suspicious1.map(_.toNLP))
				val lsRDD2 = algo2.km(2, subNumIter, subSeed)
				val summary2 = wrapSummary(lsRDD2, numBad)
				LogHelper.log(summary1, s"res of $model $k")
				LogHelper.log(summary2, s"subres of $model $k")
				algo2.releaseRDD
			}
		}

		algo1.releaseRDD
	}

	private def wrapSummary(ls: RDD[LabeledSet], numBad: Long) = {
		ls.mapPartitions(_.map(x => (x.prediction, x.label)))
			.combineByKey(
				(v: Int) => Seq(v),
				(c: Seq[Int], v: Int) => c :+ v,
				(c1: Seq[Int], c2: Seq[Int]) => c1 ++ c2)
			.mapValues{ x => 
				val counters = x.groupBy(x => x).mapValues(_.size)
				val recall = 1.0 * counters.getOrElse(1, 0) / numBad
				(counters, recall)
			}.collectAsMap
	}

	private def wrapPositive(ls: RDD[LabeledSet], summary: sMap[Int, (imMap[Int, Int], Double)]) = {
		val suspiciousClu = summary.maxBy(_._2._2)._1
		val suspiciousList = ls.filter(_.prediction == suspiciousClu)
		suspiciousList
	}

}