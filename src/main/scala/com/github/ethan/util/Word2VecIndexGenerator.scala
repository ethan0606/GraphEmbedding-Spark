package com.github.ethan.util

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Word2VecIndexGenerator {

	def vecToSeq: UserDefinedFunction = udf((x: DenseVector) => x.values.toSeq)

	def generateIndex(topK: Int,
										vec1: DataFrame,
										vec2: DataFrame,
										w1: String,
										v1: String,
										w2: String,
										v2: String): DataFrame = {

		val wordVecCollection = vec1.collect()
		val size = wordVecCollection.length
		val vectorSize = wordVecCollection(0).getAs[Seq[Double]](v1).length
		val wordList = new Array[String](size)
		val wordVectors = new Array[Float](size * vectorSize)

		for (i <- wordVecCollection.indices) {
			wordList(i) = wordVecCollection(i).getAs[String](w1)
			val feaVec = wordVecCollection(i).getAs[Seq[Double]](v1).map(_.toFloat)
			for (j <- feaVec.indices) {
				wordVectors(i * vectorSize + j) = feaVec(j)
			}
		}

		val m = new Word2VecModelV2(wordList, wordVectors)
		val spark = vec1.sparkSession
		val sc = vec1.sparkSession.sparkContext
		sc.broadcast(m)

		import spark.implicits._
		val result = vec2.map(x => {
			val id = x.getAs[String](w2)
			val feature = x.getAs[Seq[Double]](v2).toArray
			val featureAsDV = new DenseVector(feature)
			val result = m.findSynonyms(featureAsDV, topK, Some(id))
			val simId = result.map(_._1)
			val simScore = result.map(_._2)
			(id, simId, simScore)
		}).toDF("id", "sim_id", "sim_score")
		result
	}
}