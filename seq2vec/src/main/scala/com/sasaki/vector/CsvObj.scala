package com.sasaki.vector

class CsvObj(val seq: Seq[(String, Double)], val label: String) {
  	override def toString() = {
  		val detuple = seq.flatMap{case(a,b) => Seq(a,b.toString)}.mkString(",")
              s"${detuple},${label}"
        }
}

// For aligned occassion
class CsvObj2(val seq: Seq[Double], val label: String) {
  	override def toString() = {
              s"${seq.mkString(",")},${label}"
        }
}