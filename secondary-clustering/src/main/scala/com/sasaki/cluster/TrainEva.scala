package com.sasaki.cluster

import scala.collection.{Map => sMap}
import scala.collection.mutable.{Map => muMap, Buffer}
import scala.collection.immutable.{Range, Map => imMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering._

import java.text.SimpleDateFormat
import java.util.Date

import com.sasaki.utils._


/**
 * Use multi-k to cluster and select the cluster of best recall to cluster secondarily.
 * First is km or bkm, second is km only.
 */
object TrainEva {
	val evaResultPath = "hdfs:///netease/ver2/dimred/eva"

	val numIter = 100
	val seed = 21L    // ignored
	val minDivSize = 0.01
	val subNumIter = 100
	val subSeed = 21L
	val subMinDivSize = 0.01

	def input(data: RDD[NewLabeledPoint], batch: Range, 
			cluModel: String, rounds: Int, basePath: String) = {
		val algo1 = new ClusterWithLP(data)
		val numBad = data.filter(_.label % 10 == 1.0).count
		val sc = data.sparkContext
		val date = getTime
		val results = batch.map{ k =>
			(1 to rounds).par.map{ round =>
				val lsRDD1 = cluModel match {
					case "km" => algo1.km(k, numIter, seed)
					case "bkm" => algo1.bkm(k, numIter, seed, minDivSize)
				}
				lsRDD1.persist()
				val summary1 = wrapSummary(lsRDD1, numBad)
				val suspicious1 = wrapPositive(lsRDD1, summary1)
				// secondary clustering
				val algo2 = new ClusterWithLP(suspicious1.map(_.toNLP))
				val lsRDD2 = algo2.km(2, subNumIter, subSeed)
				val summary2 = wrapSummary(lsRDD2, numBad)
				algo2.releaseRDD
				// lsRDD1.repartition(1).saveAsTextFile(s"${basePath}/${date}/k${k}/round${round}/res-1")
				// lsRDD2.repartition(1).saveAsTextFile(s"${basePath}/${date}/k${k}/round${round}/res-2")
				report(Array(cluModel, k.toString, round.toString, summary1.toString, summary2.toString))
			}.seq.reduce(_ ++ _)
		}.reduce(_ ++ _)
		sc.parallelize(results, 1).saveAsTextFile(s"${basePath}/${date}/report")
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
				val precision = 1.0 * counters.getOrElse(1, 0) / counters.values.sum
				(counters, recall, precision)
			}.collectAsMap
	}

	private def wrapPositive(ls: RDD[LabeledSet], 
			summary: sMap[Int, (imMap[Int, Int], Double, Double)]) = {
		val suspiciousClu = summary.maxBy(_._2._2)._1
		val suspiciousList = ls.filter(_.prediction == suspiciousClu)
		suspiciousList
	}

	private def report(output: Array[String]) = {
		val buf = Buffer[String]()
		buf += "====================================="
		buf += s"Model --> ${output(0)}"
		buf += s"K --> ${output(1)}"
		buf += s"Round --> ${output(2)}"
		buf += s"First clustering --> ${output(3)}"
		buf += s"Secondary clustering --> ${output(4)}"
		buf.toArray
	}

	def getTime = {
  		val dt = new Date()
     		val sdf = new SimpleDateFormat("MMdd-HHmm")
     		sdf.format(dt)
  	}

}