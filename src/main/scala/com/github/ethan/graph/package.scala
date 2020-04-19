package com.github.ethan

package object graph {

	case class NodeAttr(var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
											var J: Array[Int] = Array.empty[Int],
											var q: Array[Double] = Array.empty[Double]) extends Serializable

	case class EdgeAttr(var dstNeighbors: Array[Long] = Array.empty[Long],
											var J: Array[Int] = Array.empty[Int],
											var q: Array[Double] = Array.empty[Double]) extends Serializable

}
