package org.apache.spark.graphx {
  import com.ibm.crail.benchmarks.LogTrait
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
  import org.apache.spark.storage.StorageLevel

  class AuxGraphLoader extends LogTrait{
    def edgeListFile(
                      sc: SparkContext,
                      path: String,
                      canonicalOrientation: Boolean = false,
                      numEdgePartitions: Int = -1,
                      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[Int, Int] =
    {
      // Parse the edge data table directly into edge partitions
      val lines =
        if (numEdgePartitions > 0) {
          sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
        } else {
          sc.textFile(path)
        }
      step("[AuxGraphLoader]Edge List Text File Read")

      val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
        val builder = new EdgePartitionBuilder[Int, Int]
        iter.foreach { line =>
          if (!line.isEmpty && line(0) != '#') {
            val lineArray = line.split("\\s+")
            if (lineArray.length < 2) {
              throw new IllegalArgumentException("Invalid line: " + line)
            }
            val srcId = lineArray(0).toLong
            val dstId = lineArray(1).toLong
            if (canonicalOrientation && srcId > dstId) {
              builder.add(dstId, srcId, 1)
            } else {
              builder.add(srcId, dstId, 1)
            }
          }
        }
        Iterator((pid, builder.toEdgePartition))
      }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
      step("[AuxGraphLoader]Edge Partition Build From Textfile")
      edges.count()

      GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
        vertexStorageLevel = vertexStorageLevel)
    }
  }
}