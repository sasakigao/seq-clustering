package com.sasaki.utils


/**
 * Log the object.
 */
object MoreEvents {

	def expand(line: String) = {
		val tokens = line.split("@")
		val motion = tokens(1)
		val params = tokens.last.split(",,")
		val prefix = motion match {
			case "5300100" => params(0)
			case "5300230" => params(2)
			case "5300105" => params(2)
			case "5300001" => params(3)
			case "5300005" => params(2)
			case "5300020" => params(3)
			case "5300025" => params(2)
			case "5300045" => params(2)
			case "5300040" => params(2)
			case "5300055" => params(3)
			case "5300060" => params(3)
			case "5300205" => params(2)
			case _ => ""
		}
		s"${prefix}${motion}"
	}

}