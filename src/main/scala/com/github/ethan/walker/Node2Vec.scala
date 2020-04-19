package com.github.ethan.walker

import com.github.ethan.graph.{AliasOps, NodeAttr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class Node2Vec extends RandomWalk {

	def randomWalk(dataFrame: DataFrame): DataFrame = {

		val spark = dataFrame.sparkSession
		import spark.implicits._

		val graph = initGraph(dataFrame)

		val edges = graph.edges.map { x =>
			(x.srcId + "," + x.dstId, x.attr)
		}.repartition(1000).cache()
		edges.first()

		val vertices = graph.vertices.cache()
		vertices.first()

		var result: RDD[(String, ArrayBuffer[Long])] = null

		for (_ <- 0 until numWalk) {

			var path: RDD[(String, ArrayBuffer[Long])] = firstWalk(vertices)
			path.first()

			for (_ <- 1 until walkLength) {
				path = edges.join(path).map { x =>
					val edgeAttr = x._2._1
					val pathBuffer = x._2._2
					val nextNodeIndex = AliasOps.drawAlias(edgeAttr.J, edgeAttr.q)
					val nextNode = edgeAttr.dstNeighbors(nextNodeIndex)
					pathBuffer.append(nextNode)
					val currId = pathBuffer.last
					val prevId = pathBuffer(pathBuffer.length - 2)
					(prevId + "," + currId, pathBuffer)
				}
				path.first()
			}

			if (result == null) {
				result = path
				result.first()
			} else {
				result = result.union(path)
				result.first()
			}

		}
		result.map { x => x._2.map(_.toString).toArray }.toDF(outputCol)
	}

	def firstWalk(vertices: RDD[(Long, NodeAttr)]): RDD[(String, ArrayBuffer[Long])] = {
		val result = vertices.map { x =>
			val nodeAttr = x._2
			val startNode = x._1
			val nextNodeIndex = AliasOps.drawAlias(nodeAttr.J, nodeAttr.q)
			val nextNode = nodeAttr.neighbors(nextNodeIndex)._1
			val buffer = new ArrayBuffer[Long]()
			buffer.append(startNode)
			buffer.append(nextNode)
			(startNode + "," + nextNode, buffer)
		}
		result
	}
}
