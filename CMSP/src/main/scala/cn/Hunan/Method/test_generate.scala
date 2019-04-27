package cn.Hunan.Method

import cn.Hunan.StructData.Edge

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
//import scala.collection.mutable.Map

class test_generate extends Serializable {
  def generate(neighbored: Map[String, ArrayBuffer[Edge]], node_Type: Array[Int], neighbor: Array[ArrayBuffer[Int]]): Array[ArrayBuffer[Int]] = {
    val random_neighbore = new Array[ArrayBuffer[Int]](neighbor.length)

    for (i <- neighbor.indices) {
      val arr = new ArrayBuffer[Int]()
      neighbor(i).foreach(x => {
        arr += x
      })
      random_neighbore(i) = arr
    }

    val itKey = neighbored.keySet
    for (key <- itKey) {
      val currentEdgeList: ArrayBuffer[Edge] = neighbored(key).map(n => {
        new Edge(n.sourcePoint, n.endPoint)
      })
      way_shuffleEdges(currentEdgeList,key.split("_")(1),neighbor,random_neighbore)
    }
    random_neighbore
  }

  def way_shuffleEdges(edgeList: ArrayBuffer[Edge], edgeType: String, neighbor: Array[ArrayBuffer[Int]], random_neighbore: Array[ArrayBuffer[Int]]): Unit = {
    var failedAttempts = 0
    val maxTrials = 100

    val shuffledEdges = new ArrayBuffer[Edge]()
    val rd = new Random()
    while (failedAttempts < maxTrials && edgeList.nonEmpty) {
      var firstPos = -1
      var secondPos = -1
      var firstEdge: Edge = null
      var secondEdge: Edge = null
      var switchFlag = false
      val prob: Double = edgeList.size.toDouble / (edgeList.size + shuffledEdges.size).toDouble
      if (rd.nextDouble() < prob) {
        firstPos = rd.nextInt(edgeList.size)
        secondPos = rd.nextInt(edgeList.size)
        firstEdge = edgeList(firstPos)
        secondEdge = edgeList(secondPos)
      } else {
        firstPos = rd.nextInt(edgeList.size)
        secondPos = rd.nextInt(shuffledEdges.size)
        firstEdge = edgeList(firstPos)
        secondEdge = shuffledEdges(secondPos)

        switchFlag = true
      }
      if (firstEdge.sourcePoint == secondEdge.sourcePoint || firstEdge.endPoint == secondEdge.endPoint) {
        failedAttempts = failedAttempts + 1
      } else {
        val copyFirstEdge = Edge(firstEdge.sourcePoint, firstEdge.endPoint)
        val copySecondEdge = Edge(secondEdge.sourcePoint, secondEdge.endPoint)

        //开始交换两条边
        firstEdge.endPoint = copySecondEdge.endPoint
        secondEdge.endPoint = copyFirstEdge.endPoint
        var successFlag = true
        if (random_neighbore(firstEdge.sourcePoint).contains(firstEdge.endPoint) || random_neighbore(secondEdge.sourcePoint).contains(secondEdge.endPoint)
          || random_neighbore(firstEdge.endPoint).contains(firstEdge.sourcePoint) || random_neighbore(secondEdge.endPoint).contains(secondEdge.sourcePoint)
          || firstEdge.sourcePoint == firstEdge.endPoint || secondEdge.sourcePoint == secondEdge.endPoint) {
          successFlag = false
        } else {
          if (edgeType.compareTo("10") == 0) {
            if (neighbor(firstEdge.sourcePoint).contains(firstEdge.endPoint) || neighbor(secondEdge.sourcePoint).contains(secondEdge.endPoint)) {
              successFlag = false
            }
          } else {
            if (neighbor(firstEdge.sourcePoint).contains(firstEdge.endPoint) || neighbor(secondEdge.sourcePoint).contains(secondEdge.endPoint)
              || neighbor(firstEdge.endPoint).contains(firstEdge.sourcePoint) || neighbor(secondEdge.endPoint).contains(secondEdge.sourcePoint)) {
              successFlag = false
            }
          }
        }
        if (successFlag) {
          updateRandomMatrix(firstEdge, secondEdge, copyFirstEdge, copySecondEdge, edgeType, random_neighbore)
          if (!switchFlag) {
            if (firstPos > secondPos) {
              edgeList.remove(firstPos)
              edgeList.remove(secondPos)
            } else {
              edgeList.remove(secondPos)
              edgeList.remove(firstPos)
            }
            shuffledEdges += firstEdge
            shuffledEdges += secondEdge
          } else {
            edgeList.remove(firstPos)
            shuffledEdges += firstEdge
          }
          failedAttempts = 0
        }else {
          firstEdge.endPoint = copyFirstEdge.endPoint
          secondEdge.endPoint = copySecondEdge.endPoint
          failedAttempts = failedAttempts + 1
        }
      }
    }
  }

  def updateRandomMatrix(firstEdge: Edge, secondEdge: Edge, copyFirstEdge: Edge, copySecondEdge: Edge, edgeType: String, randomMatrix: Array[ArrayBuffer[Int]]): Unit = {
    randomMatrix(firstEdge.sourcePoint) = randomMatrix(firstEdge.sourcePoint).filter(x => x != copyFirstEdge.endPoint)
    randomMatrix(secondEdge.sourcePoint) = randomMatrix(secondEdge.sourcePoint).filter(x => x != copySecondEdge.endPoint)
    randomMatrix(firstEdge.sourcePoint) += firstEdge.endPoint
    randomMatrix(secondEdge.sourcePoint) += secondEdge.endPoint
    if (edgeType.compareTo("10") != 0) {
      randomMatrix(copyFirstEdge.endPoint) = randomMatrix(copyFirstEdge.endPoint).filter(x => x != firstEdge.sourcePoint)
      randomMatrix(copySecondEdge.endPoint) = randomMatrix(copySecondEdge.endPoint).filter(x => x != secondEdge.sourcePoint)
      randomMatrix(copyFirstEdge.endPoint) += copySecondEdge.sourcePoint
      randomMatrix(copySecondEdge.endPoint) += copyFirstEdge.sourcePoint
    }
  }
}
