package com.sasaki.cluster

import scala.collection.{Map => sMap}
import scala.collection.mutable.{Map => muMap}
import scala.collection.immutable.{Map => imMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering._

import com.sasaki.utils._


object TrainEva {
	val evaResultPath = "hdfs:///netease/ver2/dimred/eva"

	def input(data: RDD[LabeledPoint]) = {
		val algo = new ClusterWithLP(data)

		(3 to 6).foreach{k =>
			val newLP = algo.km(k, 100, 21L)
			newLP.persist()
			// val checklist = makeReport(newLP)
			val evaRes = wrap(newLP)
			LogHelper.log(evaRes, s"res of km $k")
			val subRes = subTrainEva(evaRes, newLP)
			if (subRes isEmpty) {
				LogHelper.log(s"recall is low and skip the subtrain of km $k")
			} else {
				LogHelper.log(subRes, s"subres of km $k")
			}
		}

		(3 to 6).foreach{k =>
			val newLP = algo.bkm(k, 100, 21L, 0.01)
			newLP.persist()
			// val checklist = makeReport(newLP)
			val evaRes = wrap(newLP)
			LogHelper.log(evaRes, s"res of bkm $k")
			val subRes = subTrainEva(evaRes, newLP)
			if (subRes isEmpty) {
				LogHelper.log(s"recall is low and skip the subtrain of bkm $k")
			} else {
				LogHelper.log(subRes, s"subres of bkm $k")
			}
		}
		algo.releaseRDD
	}

	def subTrainEva(eva: sMap[Int, imMap[Int, Int]], data: RDD[LabeledSet]) = {
		var evaResSub = sMap[Int, imMap[Int, Int]]()
		val numPositive = eva.values.map(_.getOrElse(1, 0)).sum
		val recalls = eva.mapValues{ binmap =>
			1.0 * binmap.getOrElse(1, 0) / numPositive
		}
		val (subTrainCluster, recall) = recalls.maxBy(_._2)
		if (recall > 0.9) {
			val subData = data.filter(_.prediction == subTrainCluster).map(_.toLP)
			val algo = new ClusterWithLP(subData)
			val newLP = algo.km(2, 100, 21L)
			evaResSub = wrap(newLP)
			algo.releaseRDD
		} 
		evaResSub
	}


	// Pack the results of each cluster. prediction -> Map(label -> num)
	private def wrap(ls: RDD[LabeledSet]): sMap[Int, imMap[Int, Int]] = {
		ls.mapPartitions(_.map(x => (x.prediction, x.label)))
			.combineByKey(
				(v: Int) => Seq(v),
				(c: Seq[Int], v: Int) => c :+ v,
				(c1: Seq[Int], c2: Seq[Int]) => c1 ++ c2)
			.mapValues(_.groupBy(x => x)
			.mapValues(_.size))
			.collectAsMap
	}

	// Adjust the result to (user, label, prediction)
	private def makeReport(ls: RDD[LabeledSet]): RDD[(Long, Int, Int)] = {
		ls.mapPartitions(_.map(x => (x.id, x.label, x.prediction)))
	}

}