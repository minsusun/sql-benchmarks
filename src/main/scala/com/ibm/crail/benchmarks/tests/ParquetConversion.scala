package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  override def execute(): String = {
    val textLoadStart = System.nanoTime();
    val txtDF = spark.read.text(options.getInputFiles()(0))
    val textLoadTimeString = "Text File Load Time: " + (System.nanoTime() - textLoadStart)/1000000 + "msec";

    val parquetConversionStart = System.nanoTime();
    txtDF.write.parquet(s"${options.getInputFiles()(0)}.parquet")
    val parquetConversionTimeString = "Parquet Conversion Time: " + (System.nanoTime() - parquetConversionStart)/1000000 + "msec";

    val parquetReadStart = System.nanoTime();
    val _ = spark.read.parquet(s"${options.getInputFiles()(0)}.parquet")
    val parquetReadTimeString = "Parquet Read Time: " + (System.nanoTime() - parquetReadStart)/1000000 + "msec";

    "Ran  ParquetConversion on " +
      options.getInputFiles()(0) +
      "\n\t" + textLoadTimeString +
      "\n\t" + parquetConversionTimeString +
      "\n\t" + parquetReadTimeString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
