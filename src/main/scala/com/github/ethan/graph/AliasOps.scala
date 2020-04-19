package com.github.ethan.graph

import scala.collection.mutable.ArrayBuffer

/**
	* alias method sampling
	*/
object AliasOps {


	def normalize(probabilities: Array[Double]): Array[Double] = {
		val sum = probabilities.sum
		probabilities.map(x => x / sum)
	}

	def setupAlias(probabilities: Array[Double]): (Array[Int], Array[Double]) = {

		val K = probabilities.length
		val J = Array.fill(K)(0)
		val q = Array.fill(K)(0.0)

		val smaller = new ArrayBuffer[Int]()
		val larger = new ArrayBuffer[Int]()

		probabilities.zipWithIndex.foreach { case (prob, kk) =>
			q(kk) = K * prob
			if (q(kk) < 1.0) {
				smaller.append(kk)
			} else {
				larger.append(kk)
			}
		}

		while (smaller.nonEmpty && larger.nonEmpty) {
			val small = smaller.remove(smaller.length - 1)
			val large = larger.remove(larger.length - 1)

			J(small) = large
			q(large) = q(large) + q(small) - 1.0
			if (q(large) < 1.0) {
				smaller.append(large)
			} else {
				larger.append(large)
			}
		}
		(J, q)
	}


	def drawAlias(J: Array[Int], q: Array[Double]): Int = {
		val K = J.length
		val kk = math.floor(math.random * K).toInt
		if (math.random < q(kk)) kk
		else J(kk)
	}


	def main(args: Array[String]): Unit = {

		val probabilities = Array(0.28, 0.32, 0.1, 0.3)

		val jq = setupAlias(probabilities)
		val res = new ArrayBuffer[Int]()
		for (_ <- 0 until 1000) {
			val index = drawAlias(jq._1, jq._2)
			res.append(index)
		}

		for (i <- probabilities.indices) {
			println(i + ": " + res.count(x => x == i))
		}

	}

}
