package com.github.ethan.util

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.linalg.DenseVector

class Word2VecModelV2(val wordList: Array[String],
											val wordVectors: Array[Float]) extends Serializable {

	private val numWords = wordList.length
	private val vectorSize = wordVectors.length / numWords

	private val wordVecNorms: Array[Float] = {
		val wordVecNorms = new Array[Float](numWords)
		var i = 0
		while (i < numWords) {
			val vec = wordVectors.slice(i * vectorSize, i * vectorSize + vectorSize)
			wordVecNorms(i) = blas.snrm2(vectorSize, vec, 1)
			i += 1
		}
		wordVecNorms
	}

	def findSynonyms(vector: DenseVector,
									 num: Int,
									 wordOpt: Option[String]): Array[(String, Double)] = {
		require(num > 0, "Number of similar words should > 0")

		val fVector = vector.toArray.map(_.toFloat)
		val cosineVec = new Array[Float](numWords)
		val alpha: Float = 1
		val beta: Float = 0

		val vecNorm = blas.snrm2(vectorSize, fVector, 1)

		if (vecNorm != 0.0f) {
			blas.sscal(vectorSize, 1 / vecNorm, fVector, 0, 1)
		}

		blas.sgemv(
			"T", vectorSize, numWords, alpha, wordVectors, vectorSize, fVector, 1, beta, cosineVec, 1)

		var i = 0
		while (i < numWords) {
			val norm = wordVecNorms(i)
			if (norm == 0.0f) {
				cosineVec(i) = 0.0f
			} else {
				cosineVec(i) /= norm
			}
			i += 1
		}

		val pq = new BoundedPriorityQueue[(String, Float)](num + 1)(Ordering.by(_._2))

		var j = 0
		while (j < numWords) {
			pq += Tuple2(wordList(j), cosineVec(j))
			j += 1
		}

		val scored = pq.toSeq.sortBy(-_._2)

		val filtered = wordOpt match {
			case Some(w) => scored.filter(tup => w != tup._1)
			case None => scored
		}

		filtered
			.take(num)
			.map { case (word, score) => (word, score.toDouble) }
			.toArray
	}
}
