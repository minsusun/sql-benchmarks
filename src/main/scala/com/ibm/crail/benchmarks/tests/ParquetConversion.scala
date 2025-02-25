package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{Action, Noop, ParseOptions, SQLTest}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, GraphLoader, VertexRDD}
import org.apache.spark.sql.SparkSession

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {

  private var stepTime = System.nanoTime();

  def step(StepName: String): String = {
    val now = System.nanoTime();
    val info = s"\n\t\t Step '" + StepName + s"': ${(now - stepTime)/1000000} ms"
    stepTime = now;
    info
  }

  override def execute(): String = {
    var s = "Ran Parquet Conversion on" + options.getInputFiles()(0)

    val graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0)).cache()
    s += step("[GraphX]Graph Load")

    val edgeDF = spark.createDataFrame(graph.edges)
    s += step("[Edge]RDD->DF")

    val edgeParquetName = s"${options.getInputFiles()(0)}.edge.parquet"
    edgeDF.write.parquet(edgeParquetName)
    s += step("[Edge]Saving Parquet")

    val vertexDF = spark.createDataFrame(graph.edges)
    s += step("[Vertex]RDD->DF")

    val vertexParquetName = s"${options.getInputFiles()(0)}.vertex.parquet"
    vertexDF.write.parquet(vertexParquetName)
    s += step("[Vertex]Saving Parquet")

    val edgeRDD = spark.read.parquet(edgeParquetName)
      .rdd
      .map(row => Edge[Long](row.getLong(0), row.getLong(1)))
    s += step("[Edge]Restore RDD from parquet")

    val vertexRDD = spark.read.parquet(vertexParquetName)
      .rdd
      .map(row => (row.getLong(0), 1))
    s += step("[Vertex]Restore RDD from parquet")

    val e = EdgeRDD.fromEdges(edgeRDD)
    val v = VertexRDD.fromEdges(e, 1, 1)
    s += step("Convert to EdgeRDD and VertexRDD")

    val reConstructedGraph = GraphImpl.fromExistingRDDs(v, e)
    s += step("[GraphX]Construct Graph with RDDs")

    s
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "ParquetConversion on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder()
    sb.mkString
  }
}
