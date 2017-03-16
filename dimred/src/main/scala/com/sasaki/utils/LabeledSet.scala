package com.sasaki.utils

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors}

import com.sasaki.utils._


case class LabeledSet(
		val vec: Array[Double], 
		val id: Long, 
		val label: Int, 
		val prediction: Int) {

	// Transform LabeledSet to LabeledPoint.
	def toNLP = {
		val labelMix = (id * 10 + label).toDouble
		new NewLabeledPoint(labelMix, Vectors.dense(vec))
	}
	
}