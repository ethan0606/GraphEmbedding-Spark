package com.github.ethan.graph


import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
	* initial graph with node alias and edge alias
	*/
object GraphOps {

	def setupEdgeAlias(p: Double = 1.0, q: Double = 1.0)(srcId: Long,
																											 srcNeighbors: Array[(Long, Double)],
																											 dstNeighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {

		val unNormalizedProbabilities = dstNeighbors.map { case (dstNeighborId, weight) =>
			var unNormProb = weight / q
			if (srcId == dstNeighborId) unNormProb = weight / p
			else if (srcNeighbors.exists(_._1 == dstNeighborId)) unNormProb = weight
			unNormProb
		}
		AliasOps.setupAlias(AliasOps.normalize(unNormalizedProbabilities))
	}


	def setupNodeAlias(neighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
		val unNormalizedProbabilities = neighbors.map(_._2)
		AliasOps.setupAlias(AliasOps.normalize(unNormalizedProbabilities))
	}


	def initTransitionProb(spark: SparkSession,
												 edges: RDD[Edge[EdgeAttr]],
												 vertices: RDD[(VertexId, NodeAttr)],
												 p: Double,
												 q: Double): Graph[NodeAttr, EdgeAttr] = {

		val bcP = spark.sparkContext.broadcast(p)
		val bcQ = spark.sparkContext.broadcast(q)

		val graph = Graph(vertices, edges).mapVertices[NodeAttr] {
			case (_, nodeAttr) =>
				val (j, q) = GraphOps.setupNodeAlias(nodeAttr.neighbors)
				nodeAttr.J = j
				nodeAttr.q = q
				nodeAttr
		}.mapTriplets {

			edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>

			val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(
				edgeTriplet.srcId,
				edgeTriplet.srcAttr.neighbors,
				edgeTriplet.dstAttr.neighbors)

			edgeTriplet.attr.J = j
			edgeTriplet.attr.q = q

			edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)

			edgeTriplet.attr
		}

		graph
	}

}
