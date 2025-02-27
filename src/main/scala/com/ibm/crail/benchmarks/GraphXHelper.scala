package org.apache.spark.graphx {

  import org.apache.spark.graphx.impl.EdgePartitionBuilder

  class GraphXHelper {
    val instance = new EdgePartitionBuilder[Int, Int]()
    val method = instance.toEdgePartition
  }
}