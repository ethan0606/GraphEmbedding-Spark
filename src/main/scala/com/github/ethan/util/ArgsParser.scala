package com.github.ethan.util

import org.apache.commons.cli._

class ArgsParser(args: Array[String]) {
	val options = new Options()
	val parser = new PosixParser()
	var cmd: CommandLine = _

	def init(keys: Array[String]): Unit = {
		for (key <- keys) {
			options.addOption(key, true, "")
		}
		cmd = parser.parse(options, args)
	}

	def getInt(key: String): Int = {
		cmd.getOptionValue(key).toInt
	}

	def getString(key: String): String = {
		cmd.getOptionValue(key)
	}

	def getDouble(key: String): Double = {
		cmd.getOptionValue(key).toDouble
	}

	def print(keys: Array[String]): Unit = {
		for (key <- keys) {
			println(key + ":" + getString(key))
		}
	}
}

object ArgsParser {

	def get(args: Array[String],
					keys: Array[String]): ArgsParser = {
		val argParser = new ArgsParser(args)
		argParser.init(keys)
		argParser.print(keys)
		argParser
	}

}