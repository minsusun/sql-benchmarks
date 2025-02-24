package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  override def execute(): String = {

    val txtDF = spark.read.text(options.getInputFiles()(0))
    txtDF.write.parquet(s"${options.getInputFiles()(0)}.parquet")

    "Ran  ParquetConversion on " + options.getInputFiles()(0)
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
