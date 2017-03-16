package com.sasaki.utils

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors}
import org.apache.spark.rdd.RDD

import com.sasaki.utils._


case class VecPack(
		val label: String,
		var feaMap: Map[String, Double]) {

	def setFeaMap(feaMap: Map[String, Double]): this.type = {
    		this.feaMap = feaMap
    		this
  	}

  	override def toString = {
  		s"${label},${feaMap}"
  	}
	
}