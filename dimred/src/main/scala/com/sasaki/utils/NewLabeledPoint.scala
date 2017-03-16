package com.sasaki.utils

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector


class NewLabeledPoint(label: Double, features: Vector) 
		extends LabeledPoint(label, features) {
	override def toString = {
		s"${features.toArray.mkString(",")},${label.toLong}"
	}
}