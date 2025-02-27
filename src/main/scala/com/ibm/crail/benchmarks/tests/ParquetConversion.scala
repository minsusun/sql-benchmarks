package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{ParseOptions, SQLTest}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, EdgeRDD, GraphLoader, GraphXHelper, VertexRDD, Graph}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

class ParquetConversion (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {

  private var s = "";
  private var stepTime = System.nanoTime();

  def loader(sc: SparkContext, path: String): Graph[Int, Int] = {
    val edgeStorageLevel = StorageLevel.MEMORY_ONLY
    val vertexStorageLevel = StorageLevel.MEMORY_ONLY

    val lines = sc.textFile(path)
    s += step("[AuxGraphLoader]Spark TextFile Read")

    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val h = new GraphXHelper()
      val b = h.instance

//      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          //            builder.add(srcId, dstId, 1)
          b.add(srcId, dstId, 1)
        }
      }
      //        Iterator((pid, builder.toEdgePartition))
      Iterator((pid, h.method))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    s += step("[AuxGraphLoader]Edge Partition Build From Textfile")

    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
  }

  def step(StepName: String): String = {
    val now = System.nanoTime();
    val info = s"\n\t\t\t\tStep '" + StepName + s"': ${(now - stepTime)/1000000} ms"
    stepTime = now;
    info
  }

  override def execute(): String = {
    s = "Ran Parquet Conversion on" + options.getInputFiles()(0)
    val graph = loader(spark.sparkContext, options.getInputFiles()(0)).cache()
//    val graph = GraphLoader.edgeListFile(spark.sparkContext, options.getInputFiles()(0)).cache()
//    s += step("[GraphX]Graph Load")

    val edgeDF = spark.createDataFrame(graph.edges)
    s += step("[Edge]RDD->DF")

    val edgeParquetName = s"${options.getInputFiles()(0)}.edge.parquet"
    edgeDF.write.mode(SaveMode.Overwrite).parquet(edgeParquetName)
    s += step("[Edge]Saving Parquet")

    val edgeRDD = spark.read.parquet(edgeParquetName)
      .rdd
      .map(row => Edge[Long](row.getLong(0), row.getLong(1)))
    s += step("[Edge]Restore RDD from parquet")

    val e = EdgeRDD.fromEdges(edgeRDD)
    val v = VertexRDD.fromEdges(e, 1, 1)
    s += step("Convert to EdgeRDD and VertexRDD")

    val reConstructedGraph = GraphImpl.fromExistingRDDs(v, e).cache()
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
