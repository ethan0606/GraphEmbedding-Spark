package com.github.ethan

import com.github.ethan.graph.{AliasOps, NodeAttr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class DeepWalk extends RandomWalk {

	/**
		* generate the deep walk sequences
		*
		* @param dataFrame input dataFrame
		* @return
		*/
	def randomWalk(dataFrame: DataFrame): DataFrame = {

		val spark = dataFrame.sparkSession
		val sc = spark.sparkContext
		import spark.implicits._

		val graph = initGraph(dataFrame)

		val vertices = graph.vertices.repartition(1000).cache()

		vertices.first() // perform action

		val nodeMap = vertices.collect().toMap[Long, NodeAttr]
		val bcNodeMap = sc.broadcast(nodeMap)

		var result: RDD[Array[Long]] = null

		for (_ <- 0 until numWalk) {
			val sequence = vertices.map { x =>
				val path = new ArrayBuffer[Long]()
				val startNode = x._1
				path.append(startNode)
				for (_ <- 1 until walkLength) {
					val currNode = path.last
					val currNodeAttr = bcNodeMap.value.getOrElse(currNode, null)
					val nextNodeIndex = AliasOps.drawAlias(currNodeAttr.J, currNodeAttr.q)
					val nextNode = currNodeAttr.neighbors(nextNodeIndex)._1
					path.append(nextNode)
				}
				path.toArray
			}

			if (result == null) {
				result = sequence
				result.first()
			} else {
				result = result.union(sequence)
				result.first()
			}
		}

		result.toDF(outputCol)
	}

}
