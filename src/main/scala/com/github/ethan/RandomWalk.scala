package com.github.ethan

import com.github.ethan.graph.{EdgeAttr, GraphOps, NodeAttr}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait RandomWalk extends Serializable {

	var p: Double = 1.0
	var q: Double = 1.0
	var numWalk: Int = 10
	var walkLength: Int = 10
	var bcMaxDegree:Int = 30


	var srcCol:String = "src"
	var dstCol:String = "dst"
	var weightCol:String = "weight"
	var outputCol:String = "sequence"

	/**
		* initialize the graph
		* @param dataFrame input dataFrame
		*/
	def initGraph(dataFrame: DataFrame):Graph[NodeAttr, EdgeAttr] = {
		val spark = dataFrame.sparkSession
		import spark.implicits._

		val edges = dataFrame.map(x=> {
			val src = x.getAs[Long](srcCol)
			val dst = x.getAs[Long](dstCol)
			Edge(src, dst, EdgeAttr())
		}).rdd

		val vertices = dataFrame.groupBy(srcCol).agg(
			collect_list(dstCol),
			collect_list(weightCol)
		).map(x=> {
			val src = x.getLong(0)
			val dstSeq = x.getSeq[Long](1)
			val weightSeq = x.getSeq[Double](2)
			val dstWeight = dstSeq.zip(weightSeq).sortBy(_._2).reverse.take(bcMaxDegree)
			(src, NodeAttr(neighbors = dstWeight.map(x=> (x._1, x._2)).toArray))
		}).rdd

		GraphOps.initTransitionProb(spark, edges, vertices, p, q)

	}

	def setBcMaxDegree(bcMaxDegree: Int):this.type ={
		this.bcMaxDegree = bcMaxDegree
		this
	}

	def setP(p: Double): this.type = {
		this.p = p
		this
	}

	def setQ(p: Double): this.type = {
		this.q = q
		this
	}

	def setNumWalk(numWalk: Int): this.type = {
		this.numWalk = numWalk
		this
	}

	def setWalkLength(walkLength: Int): this.type = {
		this.walkLength = walkLength
		this
	}

	def setSrcCol(srcCol:String):this.type  = {
		this.srcCol = srcCol
		this
	}

	def setDstCol(dstCol:String):this.type  = {
		this.dstCol = dstCol
		this
	}

	def setWeightCol(weightCol:String):this.type  = {
		this.weightCol = weightCol
		this
	}

	def setOutputCol(outputCol:String):this.type  = {
		this.outputCol = outputCol
		this
	}

}
