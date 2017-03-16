package com.sasaki.cluster

import scala.collection.{Map => sMap}
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
	val numIter = 100
	val seed = 21L
	val minDivSize = 0.01
	val subNumIter = 100
	val subSeed = 21L
	val subMinDivSize = 0.01

	// Receive params for user input
	def input(data: RDD[NewLabeledPoint], path: String, modelStr: String, k: Int) = {
		val sc = data.sparkContext
		val algo1 = new ClusterWithLP(data)
		val lsRDD1 = modelStr match {
			case "km" => algo1.km(k, numIter, seed)
			case "bkm" => algo1.bkm(k, numIter, seed, minDivSize)
		} 
		lsRDD1.persist()
		val numBad = lsRDD1.filter(_.label == 1.0).count
		val summary1 = wrapSummary(lsRDD1, numBad)
		val suspicious1 = wrapPositive(lsRDD1, summary1)
		algo1.releaseRDD

		val algo2 = new ClusterWithLP(suspicious1.map(_.toNLP))
		val lsRDD2 = algo2.km(2, subNumIter, subSeed)
		val summary2 = wrapSummary(lsRDD2, numBad)
		val suspicious2 = wrapPositive(lsRDD2 , summary2)

		val checklist = makeReport(suspicious2, lsRDD1)
		sc.parallelize(conclude(summary1, summary2, numBad))
			.saveAsTextFile(s"${path}/${modelStr}/eva")
		checklist.repartition(1).saveAsTextFile(s"${path}/${modelStr}/checklist")
		algo2.releaseRDD
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

	// Adjust the result to (user, label, prediction)
	private def makeReport(posi: RDD[LabeledSet], ls: RDD[LabeledSet]): RDD[(Long, Int, Int)] = {
		val idSet = posi.map(_.id).collect.toSet
		ls.mapPartitions{ iter =>
			iter.map{ x =>
				val id = x.id
				val prediction = if (idSet contains id) 1 else 0
				val label = x.label
				(id, label, prediction)
			}
		}
	}

	import collection.mutable.Buffer
	private def conclude(summary1: sMap[Int, (imMap[Int, Int], Double)], 
			summary2: sMap[Int, (imMap[Int, Int], Double)], 
			numBad: Long) = {
		val lineBuf = Buffer[String]()
		lineBuf += "---------------------------First clustering round--------------------------"
		summary1.foreach(lineBuf += _.toString)
		lineBuf += "------------------------Second clustering round------------------------"
		summary2.foreach(lineBuf += _.toString)
		val target1 = summary1.maxBy(_._2._2)
		val target2 = summary2.maxBy(_._2._2)
		lineBuf += "-----------------------------------Summary-----------------------------------"
		lineBuf += s"Finished. Cluster ${target1._1} of 1st round, ${target2._1} of 2nd round is targetted."
		lineBuf += s"There are total $numBad bad guys, and ${target2._2._1(1)} of them detected, recall is ${target2._2._2}"
		lineBuf.toArray
	}

}