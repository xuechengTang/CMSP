package cn.Hunan

import cn.Hunan.Method._
import cn.Hunan.StructData.{Edge, Node, nodePartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

object CMSP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CMSP")
     .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val motif_Size = sc.broadcast(args(1).toInt)
    val rdd = sc.parallelize(1 to 1000, args(2).toInt)


    val raw_Data = sc.textFile(args(0)).map(line => {
      val fields = line.split("\t")
      (Array((fields(0).toInt, fields(2).toInt), (fields(1).toInt, fields(3).toInt)), Array((fields(0).toInt, (fields(1).toInt, 1)), (fields(1).toInt, (fields(0).toInt, 0))))
    })

    val all_Node: RDD[(Int, Int)] = raw_Data.flatMap(_._1).distinct(args(2).toInt)

    val rule_Nodes = sc.broadcast(all_Node.collect().sortBy(_._1).map(_._2))
    //val o = all_Node1.collect().sortBy(_._1).map(_._2)
    val calForNeighbor = raw_Data.flatMap(_._2).groupByKey().map(x => {
      val neighbor = new ArrayBuffer[Int]()
      val neighbored = new ArrayBuffer[Int]()
      x._2.foreach(y => {
        if (y._2 == 1) {
          neighbor += y._1
        }
        neighbored += y._1
      })
      ((x._1, neighbor.distinct.sorted), (x._1, neighbored.distinct.sorted))
    }).cache()
    //val arrLength = sc.broadcast(calForNeighbor.count())
    val adj = calForNeighbor.map(_._1).map(x => {
      var a = Array[Int]()
      if (x._2.nonEmpty) {
        a = new Array[Int](x._2.last + 1)
        x._2.foreach(y => {
          a(y) = 1
        })
      }
      (x._1, a)
    }).collect.sortBy(_._1).map(_._2)
    val adjMatrix = sc.broadcast(adj)
    ///val edges1 = calForNeighbor.map(_._1).distinct(args(2).toInt).collect().sortBy(_._1).map(_._2)
    val final_neighbor1 = calForNeighbor.map(_._2).distinct().collect().sortBy(_._1).map(_._2)
    //val neighborNode = sc.broadcast(edges1)
    val neighboredNode = sc.broadcast(final_neighbor1)

    val prepareForRandom = calForNeighbor.map(_._1).flatMap(x => {
      x._2.flatMap(y => {
        val edgeListMap = Map[String, ArrayBuffer[Edge]]()
        if (adjMatrix.value(y).length > x._1 && adjMatrix.value(y)(x._1) == 1) {
          if (x._1 < y) {
            var regTypeKey = rule_Nodes.value(x._1).toString + rule_Nodes.value(y)
            regTypeKey += "_" + "11"
            //进行判断是否存在，这个边类型
            if (edgeListMap.contains(regTypeKey)) {
              val curRegArray: ArrayBuffer[Edge] = edgeListMap(regTypeKey)
              val nt = Edge(x._1, y)
              curRegArray += nt
            } else {
              val curRegArray = new ArrayBuffer[Edge]()
              val nt = Edge(x._1, y)
              curRegArray += nt
              edgeListMap.put(regTypeKey, curRegArray)
            }
          }
        } else {
          var regTypeKey = rule_Nodes.value(x._1).toString + rule_Nodes.value(y)
          regTypeKey += "_" + "10"
          //进行判断是否存在，这个边类型
          if (edgeListMap.contains(regTypeKey)) {
            val curRegArray: ArrayBuffer[Edge] = edgeListMap(regTypeKey)
            val nt = Edge(x._1, y)
            curRegArray += nt
          } else {
            val curRegArray = new ArrayBuffer[Edge]()
            val nt = Edge(x._1, y)
            curRegArray += nt
            edgeListMap.put(regTypeKey, curRegArray)
          }
        }
        edgeListMap
      })
    }).groupByKey().map(x => {
      val edgess = new ArrayBuffer[Edge]()
      x._2.foreach(y => {
        edgess ++= y
      })
      (x._1, edgess)
    })


    val a = prepareForRandom.collect
    val microRNA_RDD = all_Node.filter(_._2 == 0).cache
    val microRNA = sc.broadcast(microRNA_RDD.collect())
    val forRadom = sc.broadcast(prepareForRandom.collect.toMap)




    if (args(4).toInt == 1) {
      val enumerate_graph = microRNA_RDD.flatMap(x => {
        val Cmsp_compute = new CMSP_compute()
        // val Cmsp_compute = new compute()
        // Cmsp_compute.com(x._1, neighboredNode.value, rule_Nodes.value, motif_Size.value,adjMatrix.value)
        Cmsp_compute.com(x._1, neighboredNode.value, rule_Nodes.value, motif_Size.value)
      }).filter(_._2.nonEmpty).flatMap(x => {
        val tmp = x._2.groupBy(start_node => {
          var label = rule_Nodes.value(start_node).toString
          for (end_node <- x._1) {
            if (adjMatrix.value(start_node).length > end_node) {
              label += adjMatrix.value(start_node)(end_node)
            } else {
              label += "0"
            }
            if (adjMatrix.value(end_node).length > start_node) {
              label += adjMatrix.value(end_node)(start_node)
            } else {
              label += "0"
            }
          }
          label
        }).map(y => {
          val ints: ArrayBuffer[Int] = duplicateArrayList(x._1)
          ints += y._2(0)
          (ints.sorted, y._2.length)
        })
        tmp
      }).map(x => {
        val iso = new isomorphism()
        (iso.judge(x._1, adjMatrix.value, rule_Nodes.value), x._2)
      }).reduceByKey(_ + _).collect.toMap

      val candidata_subgraph = sc.broadcast(enumerate_graph)
      val randomNetwork_subgraph = rdd.map(x => {
        val test = new generate_Network()
        val matrix = test.generate(forRadom.value, rule_Nodes.value, adjMatrix.value)
        matrix
      }).map(x => {
        val neighbored = new Array[ArrayBuffer[Int]](x.length)
        for (i <- x.indices) {
          val arr_1 = new ArrayBuffer[Int]()
          for (j <- x.indices) {
            if (x(i).length > j && x(i)(j) != 0) {
              arr_1 += j
            }
            if (x(j).length > i && x(j)(i) != 0) {
              arr_1 += j
            }
          }
          neighbored(i) = arr_1.distinct
        }
        (neighbored, x)
      }).mapPartitions(all => {
        val t = ArrayBuffer[(ArrayBuffer[(Int, Int, (Int, Int), (Int, Int))], Int)]()
        for (x <- all) {
          microRNA.value.flatMap(y => {
            val Cmsp_compute = new CMSP_compute()
            val randomSubgraph = Cmsp_compute.com(y._1, x._1, rule_Nodes.value, motif_Size.value).filter(_._2.nonEmpty)
            randomSubgraph.flatMap(z => {
              z._2.groupBy(start_node => {
                var label = rule_Nodes.value(start_node).toString
                for (end_node <- z._1) {
                  if (x._2(start_node).length > end_node) {
                    label += x._2(start_node)(end_node)
                  } else {
                    label += "0"
                  }
                  if (x._2(end_node).length > start_node) {
                    label += x._2(end_node)(start_node)
                  } else {
                    label += "0"
                  }
                }
                label
              }).map(p => {
                val ints: ArrayBuffer[Int] = duplicateArrayList(z._1)
                ints += p._2(0)
                (ints.sorted, p._2.length)
              })
            })
          }).map(y => {
            val iso = new isomorphism()
            (iso.judge(y._1, x._2, rule_Nodes.value), y._2)
          }).groupBy(_._1).map(y => {
            val subgraphSum = y._2.map(_._2).sum
            t += ((y._1, subgraphSum))
          })
        }
        t.toIterator
      }).groupByKey().filter(x => candidata_subgraph.value.keySet.contains(x._1))
      val result = randomNetwork_subgraph.map(x => {
        val array: Array[Int] = x._2.toArray
        val mean_Frquency: Double = array.sum / 1000
        var P_value: Double = 1000 - array.length
        val Np = candidata_subgraph.value(x._1)
        var tmpDeivation: Double = (1000 - array.length) * (mean_Frquency * mean_Frquency)
        array.foreach(y => {
          tmpDeivation = tmpDeivation + (y - mean_Frquency) * (y - mean_Frquency)
          if (y >= Np) {
            P_value = P_value + 1
          }
        })
        val standardDeivation = math.sqrt(tmpDeivation / 1000)
        (x._1, (Np - mean_Frquency) / standardDeivation, (P_value + 1) / 1001, standardDeivation, candidata_subgraph.value(x._1))
      }).filter(x => x._2 > 2 && x._3 < 0.01 && x._5 > 5).collect
      //result.saveAsTextFile(args(3))
      //print("1")
    } else {

      val enumerate_graph = microRNA_RDD.flatMap(x => {
        //val Cmsp_compute = new CMSP_compute()
         val Cmsp_compute = new compute()
         Cmsp_compute.com(x._1, neighboredNode.value, rule_Nodes.value, motif_Size.value,adjMatrix.value)
        //Cmsp_compute.com(x._1, neighboredNode.value, rule_Nodes.value, motif_Size.value)
      }).filter(_._2.nonEmpty).flatMap(x => {
        val tmp = x._2.groupBy(start_node => {
          var label = rule_Nodes.value(start_node).toString
          for (end_node <- x._1) {
            if (adjMatrix.value(start_node).length > end_node) {
              label += adjMatrix.value(start_node)(end_node)
            } else {
              label += "0"
            }
            if (adjMatrix.value(end_node).length > start_node) {
              label += adjMatrix.value(end_node)(start_node)
            } else {
              label += "0"
            }
          }
          label
        }).map(y => {
          val ints: ArrayBuffer[Int] = duplicateArrayList(x._1)
          ints += y._2(0)
          (ints.sorted, y._2.length)
        })
        tmp
      }).map(x => {
        val iso = new isomorphism()
        (iso.judge(x._1, adjMatrix.value, rule_Nodes.value), x._2)
      }).reduceByKey(_ + _).collect.toMap

      val candidata_subgraph = sc.broadcast(enumerate_graph)
      //采样
      val randomNetwork_subgraph = rdd.map(x => {
        val test = new generate_Network()
        val matrix = test.generate(forRadom.value, rule_Nodes.value, adjMatrix.value)
        matrix
      }).map(x => {
        val neighbored = new Array[ArrayBuffer[Int]](x.length)
        for (i <- x.indices) {
          val arr_1 = new ArrayBuffer[Int]()
          for (j <- x.indices) {
            if (x(i).length > j && x(i)(j) != 0) {
              arr_1 += j
            }
            if (x(j).length > i && x(j)(i) != 0) {
              arr_1 += j
            }
          }
          neighbored(i) = arr_1.distinct
        }
        (neighbored, x)
      }).mapPartitions(all => {
        val t = ArrayBuffer[(ArrayBuffer[(Int, Int, (Int, Int), (Int, Int))], Int)]()
        for (x <- all) {
          microRNA.value.flatMap(y => {
            val Cmsp_compute = new compute()
            val randomSubgraph = Cmsp_compute.com(y._1, x._1, rule_Nodes.value, motif_Size.value, x._2).filter(_._2.nonEmpty)
            randomSubgraph.flatMap(z => {
              z._2.groupBy(start_node => {
                var label = rule_Nodes.value(start_node).toString
                for (end_node <- z._1) {
                  if (x._2(start_node).length > end_node) {
                    label += x._2(start_node)(end_node)
                  } else {
                    label += "0"
                  }
                  if (x._2(end_node).length > start_node) {
                    label += x._2(end_node)(start_node)
                  } else {
                    label += "0"
                  }
                }
                label
              }).map(p => {
                val ints: ArrayBuffer[Int] = duplicateArrayList(z._1)
                ints += p._2(0)
                (ints.sorted, p._2.length)
              })
            })
          }).map(y => {
            val iso = new isomorphism()
            (iso.judge(y._1, x._2, rule_Nodes.value), y._2)
          }).groupBy(_._1).map(y => {
            val subgraphSum = y._2.map(_._2).sum
            t += ((y._1, subgraphSum))
          })
        }
        t.toIterator
      }).groupByKey().filter(x => candidata_subgraph.value.keySet.contains(x._1))
      //val dddd  = randomNetwork_subgraph.collect
      val result = randomNetwork_subgraph.map(x => {
        val array: Array[Int] = x._2.toArray
        val mean_Frquency: Double = array.sum / 1000
        var P_value: Double = 1000 - array.length
        val Np = candidata_subgraph.value(x._1) //* math.pow(0.2, motif_Size.value - 2)
        var tmpDeivation: Double = (1000 - array.length) * (mean_Frquency * mean_Frquency)
        array.foreach(y => {
          tmpDeivation = tmpDeivation + (y - mean_Frquency) * (y - mean_Frquency)
          if (y >= Np) {
            P_value = P_value + 1
          }
        })
        val standardDeivation = math.sqrt(tmpDeivation / 1000)
        (x._1, (Np - mean_Frquency) / standardDeivation, (P_value + 1) / 1001, standardDeivation, candidata_subgraph.value(x._1))
      }).filter(x => x._2 > 2 && x._3 < 0.01 && x._5 > 5)//.collect
      result.saveAsTextFile(args(3))

    }




    // result.saveAsTextFile(args(3))

    sc.stop()
  }


  def duplicateArrayList(originalList: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    val newArrayList = new ArrayBuffer[Int]()
    newArrayList ++= originalList
    newArrayList.sorted
  }
}