package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{LogTrait, ParseOptions, SQLTest}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{AuxGraphLoader, Edge, EdgeRDD, Graph, GraphLoader, VertexRDD}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) with LogTrait {

  override def execute(): String = {
    var graph: Graph[Int, Int] = null
    if (options.getAuxGraphLoader) {
      val loader = new AuxGraphLoader()

      graph = loader.edgeListFile(spark.sparkContext, options.getInputFiles()(0))
      loader.step("[AuxGraphLoader]Load Graph")

      graph = graph.cache()
      loader.step("[AuxGraphLoader]Graph Cache")

      concatLog(loader.logToString)
      forceUpdate();
    } else {
      graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0))
      step("[GraphX]Load Graph")

      graph = graph.cache()
      step("[GraphX]Graph Cache")
    }

    val edgeDF = spark.createDataFrame(graph.edges)
    step("[Edge]RDD->DF")

    val edgeParquetName = s"${options.getInputFiles()(0)}.edge.parquet"
    edgeDF.write.mode(SaveMode.Overwrite).parquet(edgeParquetName)
    step("[Edge]Saving Parquet")

    val edgeRDD = spark.read.parquet(edgeParquetName)
      .rdd
      .map(row => Edge[Long](row.getLong(0), row.getLong(1)))
    step("[Edge]Restore RDD from parquet")

    val e = EdgeRDD.fromEdges(edgeRDD)
    val v = VertexRDD.fromEdges(e, 1, 1)
    step("Convert to EdgeRDD and VertexRDD")

    val reConstructedGraph = GraphImpl.fromExistingRDDs(v, e).cache()
    step("[GraphX]Construct Graph with RDDs")

    "Ran Parquet Conversion on" + options.getInputFiles()(0) + logToString
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
