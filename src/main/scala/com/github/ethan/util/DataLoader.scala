package com.github.ethan.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

	def loadEdgeList(spark: SparkSession, path: String): DataFrame = {
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		val df = spark.read.text(path).map(x => {
			val line = x.getString(0).split("\\s+")
			val src = line(0).toLong
			val dst = line(1).toLong
			val weight = if (line.length == 2) {
				1d
			} else {
				line(2).toDouble
			}
			(src, dst, weight)
		}).toDF("src", "dst", "weight")
		df
	}
}
