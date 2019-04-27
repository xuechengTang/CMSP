package cn.Hunan.Method

import cn.Hunan.StructData.Node

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}


class isomorphism() extends Serializable {
  def isomorphism_Judge(subgraph_count: (String, Int)): ArrayBuffer[(Char, Char, (Int, Int), (Int, Int))] = {
    /*val miRNA_TF = 1
    val miRNA_gene = 2
    val TF_miRNA = 3
    val TF_TF = 4
    val TF_gene = 5*/
    val lable_typeList = subgraph_count._1
    val fields = lable_typeList.split("_")
    val labels = fields(0).toCharArray
    val typeList = fields(1).toCharArray
    val adjMatrix = Array.ofDim[Int](typeList.length, typeList.length)
    var count = 0
    labels.foreach(node => {
      if (node == '0') {
        count = count + 1
      } else if (node == '1') {
        val row: Int = count / typeList.length
        val col: Int = count % typeList.length
        adjMatrix(row)(col) = 1
        count = count + 1
      }
    })
    var result = new ArrayBuffer[(Char, Char, (Int, Int), (Int, Int))]()
    for (i <- Range(0, typeList.length)) {
      for (j <- Range(0, typeList.length)) {
        if (i != j && adjMatrix(i)(j) == 1) {
          val start = typeList(i)
          val end = typeList(j)
          var input_degree_i = 0
          var output_degree_i = 0
          var input_degree_j = 0
          var output_degree_j = 0
          for (x <- Range(0, typeList.length)) {
            if (adjMatrix(i)(x) == 1) {
              output_degree_i = output_degree_i + 1
            }
            if (adjMatrix(j)(x) == 1) {
              output_degree_j = output_degree_j + 1
            }
            if (adjMatrix(x)(j) == 1) {
              input_degree_j = input_degree_j + 1
            }
            if (adjMatrix(x)(i) == 1) {
              input_degree_i = input_degree_i + 1
            }
          }
          result += ((start, end, (output_degree_i, input_degree_i), (output_degree_j, input_degree_j)))
        }
      }
    }
    result = result.sortWith((x, y) => {
      var tmp: Boolean = false
      if (x._1 < y._1) {
        tmp = true
      } else if (x._1 == y._1) {
        if (x._2 < y._2) {
          tmp = true
        } else if (x._2 == y._2) {
          if (x._3._1 < y._3._1) {
            tmp = true
          } else if (x._3._1 == y._3._1) {
            if (x._3._2 < y._3._2) {
              tmp = true
            } else if (x._3._2 == y._3._2) {
              if (x._4._1 < y._4._1) {
                tmp = true
              } else if (x._4._1 == y._4._1) {
                if (x._4._2 < y._4._2) {
                  tmp = true
                } else {
                  tmp = false
                }
              }
            } else {
              tmp = false
            }
          } else {
            tmp = false
          }
        } else {
          tmp = false
        }
      } else {
        tmp = false
      }
      tmp
    })
    result
  }

  def judge(subgraph: ArrayBuffer[Int], adjMatrix: Array[Array[Int]],ruleNode:Array[Int]): ArrayBuffer[(Int, Int, (Int, Int), (Int, Int))] = {
    val result = new ArrayBuffer[(Int, Int, (Int, Int), (Int, Int))]()
    val out_in = Map[Int, (Int, Int)]()
    subgraph.foreach(x => {
      subgraph.foreach(y => {
        if (x != y && adjMatrix(x).length > y && adjMatrix(x)(y) == 1) {
          if (out_in.keySet.contains(x)) {
            val out = out_in(x)._1 + 1
            out_in += (x -> (out, out_in(x)._2))
          } else {
            out_in += (x -> (1, 0))
          }
          if (out_in.keySet.contains(y)) {
            val in = out_in(y)._2 + 1
            out_in += (y -> (out_in(y)._1, in))
          } else {
            out_in += (y -> (0, 1))
          }
        }
      })
    })

    subgraph.foreach(x => {
      subgraph.foreach(y => {
        if (x != y && adjMatrix(x).length > y && adjMatrix(x)(y) == 1) {
          result += ((ruleNode(x),ruleNode(y),out_in(x),out_in(y)))
        }
      })
    })
    result.sortWith((x, y) => {
      var tmp: Boolean = false
      if (x._1 < y._1) {
        tmp = true
      } else if (x._1 == y._1) {
        if (x._2 < y._2) {
          tmp = true
        } else if (x._2 == y._2) {
          if (x._3._1 < y._3._1) {
            tmp = true
          } else if (x._3._1 == y._3._1) {
            if (x._3._2 < y._3._2) {
              tmp = true
            } else if (x._3._2 == y._3._2) {
              if (x._4._1 < y._4._1) {
                tmp = true
              } else if (x._4._1 == y._4._1) {
                if (x._4._2 < y._4._2) {
                  tmp = true
                } else {
                  tmp = false
                }
              }
            } else {
              tmp = false
            }
          } else {
            tmp = false
          }
        } else {
          tmp = false
        }
      } else {
        tmp = false
      }
      tmp
    })
  }

}

object isomorphism {
  def apply(): isomorphism = new isomorphism()
}
