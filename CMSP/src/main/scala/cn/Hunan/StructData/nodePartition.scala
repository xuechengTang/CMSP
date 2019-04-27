package cn.Hunan.StructData

import org.apache.spark.Partitioner

class nodePartition(num: Int) extends Partitioner with Serializable {

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Node]
    k.miRna % num
  }
}
