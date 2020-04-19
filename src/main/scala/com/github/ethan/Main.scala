package com.github.ethan

import com.github.ethan.util.{ArgsParser, DataLoader, GraphUtil, Word2VecIndexGenerator}
import com.github.ethan.walker.DeepWalk
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

	def main(args: Array[String]): Unit = {

		val options = Array(
			"dataPath",
			"embeddingPath",
			"indexPath",
			"vectorSize",
			"windowSize",
			"maxIter",
			"topK",
			"numWalk",
			"walkLength",
			"masterURL"
		)

		val argParser = ArgsParser.get(args, options)

		val dataPath = argParser.getString("dataPath")
		val embeddingPath = argParser.getString("embeddingPath")
		val indexPath = argParser.getString("indexPath")
		val vectorSize = argParser.getInt("vectorSize")
		val windowSize = argParser.getInt("windowSize")
		val numWalk = argParser.getInt("numWalk")
		val walkLength = argParser.getInt("dataPath")
		val masterURL = argParser.getString("masterURL")
		val maxIter = argParser.getInt("maxIter")

		val spark = SparkSession.builder().master(masterURL).getOrCreate()

		val data = DataLoader.loadEdgeList(spark, dataPath)

		val processedDF = GraphUtil.preProcess(data)

		val walk = new DeepWalk()
			.setNumWalk(numWalk)
			.setWalkLength(walkLength)

		val result = walk.randomWalk(processedDF).cache()
		result.first()

		val w2v = new Word2Vec()
			.setMaxSentenceLength(100)
			.setMinCount(2)
			.setWindowSize(windowSize)
			.setVectorSize(vectorSize)
			.setMaxIter(maxIter)
			.setNumPartitions(30)
			.setInputCol("sequence")
			.fit(result)

		var wv = w2v.getVectors.toDF("word", "vector")

		wv.write.mode("overwrite").save(embeddingPath)

		wv = wv.withColumn("vector", Word2VecIndexGenerator.vecToSeq(col("vector")))

		val index = Word2VecIndexGenerator.generateIndex(20, wv, wv, "word", "vector", "word", "vector")

		index.write.mode("overwrite").save(indexPath)

	}
}