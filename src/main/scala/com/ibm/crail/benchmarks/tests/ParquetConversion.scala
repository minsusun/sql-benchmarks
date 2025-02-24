package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
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

//    val graphLoadStart = System.nanoTime();
//    val graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0)).cache()
//    val graphLoadTimeString = "Graph Load Time: " + (System.nanoTime() - graphLoadStart)/1000000 + "msec";
//
//    val graphRDDtoDFStart = System.nanoTime();
//    val EdgeDF = spark.createDataFrame(graph.edges)
//    val VertexDF = spark.createDataFrame(graph.vertices)
//    val graphRDDtoDFTimeString = "Graph RDD to DF Time: " + (System.nanoTime() - graphRDDtoDFStart)/1000000 + "msec";
//
//    val graphParquetConversionStart = System.nanoTime();
//    EdgeDF.write.parquet(s"${options.getInputFiles()(0)}.edge.parquet")
//    VertexDF.write.parquet(s"${options.getInputFiles()(0)}.vertex.parquet")
//    val graphParquetConversionTimeString = "Graph DF to Parquet Time: " + (System.nanoTime() - graphParquetConversionStart)/1000000 + "msec";
//
//    val graphParquetLoadStart = System.nanoTime();
//    val EdgeRDD = spark.read.parquet(s"${options.getInputFiles()(0)}.edge.parquet")
//      .rdd
//      .map(row => Edge(row.getLong(0), row.getLong(1)))
//    val VertexRDD = spark.read.parquet(s"${options.getInputFiles()(0)}.vertex.parquet")
//      .rdd
//      .map(row => (row.getLong(0), 1))
//    val parquetGraph = new Graph(EdgeRDD, VertexRDD)

    val HiBenchStart = System.nanoTime();
    {
      val lines = spark.sparkContext.textFile(options.getInputFiles()(0), 1)
      val links = lines.map{ s =>
        val elements = s.split("\\s+")
        val parts = elements.slice(elements.length - 2, elements.length)
        (parts(0), parts(1))
      }.distinct().groupByKey().cache()
      var ranks = links.mapValues(v => 1.0)
    }
    val HiBenchTimeString = "HiBench Graph Load Time: " + (System.nanoTime() - HiBenchStart)/1000000 + "msec";
    val graphXStart = System.nanoTime();
    val graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0)).cache()
    val graphXTimeString = "GraphX Graph Load Time: " + (System.nanoTime() - HiBenchStart)/1000000 + "msec";

    "Ran  ParquetConversion on " +
      options.getInputFiles()(0) +
      "\n\t" + DFLoadTimeString +
      "\n\t" + RDDLoadTimeString +
      "\n\t" + parquetConversionTimeString +
      "\n\t" + parquetReadTimeString +
      "\n\t" + dfRDDConversionTimeString +
      "\n\t" + HiBenchTimeString +
      "\n\t" + graphXTimeString
//      "\n\t" + graphLoadTimeString +
//      "\n\t" + graphRDDtoDFTimeString +
//      "\n\t" + graphParquetConversionTimeString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
