package com.github.ethan

import org.apache.spark.sql.SparkSession

object Main {

	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder().master("local").getOrCreate()

		val df = spark.createDataFrame(
			Seq((1L, 2L, 0.5), (1L, 3L, 3.0), (2L, 3L, 1.0), (2L, 1L, 1.5))
		).toDF("src", "dst", "weight")

		val processedDF = GraphUtil.preProcess(df)

		processedDF.show()


		val walk = new Node2Vec()
			.setWalkLength(5)
			.setNumWalk(2)

		val result = walk.randomWalk(processedDF)

		result.show()


	}
}
