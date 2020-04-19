package com.github.ethan

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GraphUtil {

	// process the graph to make sure it can be fit into randomWalk
	def preProcess(dataFrame: DataFrame, srcCol: String="src", dstCol: String="dst", weightCol: String="weight"): DataFrame = {

		val spark = dataFrame.sparkSession
		import spark.implicits._

		val filteredDF = dataFrame.filter(col(srcCol) =!= col(dstCol))

		val uniqueEdges = filteredDF.map(x=> {
			val src = x.getAs[Long](srcCol)
			val dst = x.getAs[Long](dstCol)
			val weight = x.getAs[Double](weightCol)
			val srcDst = Array(src, dst).sorted.mkString(",")
			(srcDst, weight)
		}).toDF("edge", "weight")

		val weightedEdges = uniqueEdges.groupBy("edge").agg(
			sum("weight")
		).flatMap(x=> {
			val edge = x.getString(0).split(",").map(_.toLong)
			val weight = x.getDouble(1)
			Array((edge(0), edge(1), weight), (edge(1), edge(0), weight))
		}).toDF(srcCol, dstCol, weightCol)

		weightedEdges

	}
}
