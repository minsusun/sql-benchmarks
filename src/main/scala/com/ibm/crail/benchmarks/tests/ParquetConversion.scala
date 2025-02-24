package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  override def execute(): String = {
    val DFLoadStart = System.nanoTime();
    val txtDF = spark.read.text(options.getInputFiles()(0))
    val DFLoadTimeString = "Text File(spark.read.text;DF) Load Time: " + (System.nanoTime() - DFLoadStart)/1000000 + "msec";

    val RDDLoadStart = System.nanoTime();
    val txtRDD = spark.sparkContext.textFile(options.getInputFiles()(0))
    val RDDLoadTimeString = "Text File(sparkContext.textFile;RDD) Load Time: " + (System.nanoTime() - RDDLoadStart)/1000000 + "msec";

    val parquetConversionStart = System.nanoTime();
    txtDF.write.parquet(s"${options.getInputFiles()(0)}.parquet")
    val parquetConversionTimeString = "Parquet Conversion Time: " + (System.nanoTime() - parquetConversionStart)/1000000 + "msec";

    val parquetReadStart = System.nanoTime();
    val df = spark.read.parquet(s"${options.getInputFiles()(0)}.parquet")
    val parquetReadTimeString = "Parquet Read Time: " + (System.nanoTime() - parquetReadStart)/1000000 + "msec";

    val dfRDDConversionStart = System.nanoTime();
    val RDD = df.toJavaRDD;
    val dfRDDConversionTimeString = "DF to RDD Conversion Time: " + (System.nanoTime() - dfRDDConversionStart)/1000000 + "msec";

    "Ran  ParquetConversion on " +
      options.getInputFiles()(0) +
      "\n\t" + DFLoadTimeString +
      "\n\t" + RDDLoadTimeString +
      "\n\t" + parquetConversionTimeString +
      "\n\t" + parquetReadTimeString +
      "\n\t" + dfRDDConversionTimeString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
