package cn.Hunan.Method

import cn.Hunan.StructData.Edge

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class generate_Network() extends Serializable {

  def generate(prepareForRandom: Map[String, ArrayBuffer[Edge]], node_Type: Array[Int], adjMatrix: Array[Array[Int]]): Array[Array[Int]] = {
    val randomAdjmatrix = new Array[Array[Int]](adjMatrix.length)
    for (i <- adjMatrix.indices) {
      val arr = adjMatrix(i)
      val random = new Array[Int](arr.length)
      arr.copyToArray(random, 0, arr.length)
      randomAdjmatrix(i) = random
    }
    val itKey = prepareForRandom.keySet
    for (key <- itKey) {
      val currentEdgeList: ArrayBuffer[Edge] = prepareForRandom(key).map(n => {
        new Edge(n.sourcePoint, n.endPoint)
      })
      way_shuffleEdges(currentEdgeList, key.split("_")(1), adjMatrix, randomAdjmatrix)
    }
    randomAdjmatrix
  }

  def way_shuffleEdges(edgeList: ArrayBuffer[Edge], edgeType: String, adjMatrix: Array[Array[Int]], randomMatrix: Array[Array[Int]]): Unit = {
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
        if ((randomMatrix(firstEdge.sourcePoint).length > firstEdge.endPoint && randomMatrix(firstEdge.sourcePoint)(firstEdge.endPoint) > 0) || (randomMatrix(secondEdge.sourcePoint).length > secondEdge.endPoint && randomMatrix(secondEdge.sourcePoint)(secondEdge.endPoint) > 0)
          || (randomMatrix(firstEdge.endPoint).length > firstEdge.sourcePoint && randomMatrix(firstEdge.endPoint)(firstEdge.sourcePoint) > 0) || (randomMatrix(secondEdge.endPoint).length > secondEdge.sourcePoint && randomMatrix(secondEdge.endPoint)(secondEdge.sourcePoint) > 0)
          || firstEdge.sourcePoint == firstEdge.endPoint || secondEdge.sourcePoint == secondEdge.endPoint) {
          successFlag = false
        } else {
          if (edgeType.compareTo("10") == 0) {
            if ((adjMatrix(firstEdge.sourcePoint).length > firstEdge.endPoint && adjMatrix(firstEdge.sourcePoint)(firstEdge.endPoint) > 0) || (adjMatrix(secondEdge.sourcePoint).length > secondEdge.endPoint && adjMatrix(secondEdge.sourcePoint)(secondEdge.endPoint) > 0)) {
              successFlag = false
            }
          } else {
            if ((adjMatrix(firstEdge.sourcePoint).length > firstEdge.endPoint && adjMatrix(firstEdge.sourcePoint)(firstEdge.endPoint) > 0) || (adjMatrix(secondEdge.sourcePoint).length > secondEdge.endPoint && adjMatrix(secondEdge.sourcePoint)(secondEdge.endPoint) > 0)
              || (adjMatrix(firstEdge.endPoint).length > firstEdge.sourcePoint && adjMatrix(firstEdge.endPoint)(firstEdge.sourcePoint) > 0) || (adjMatrix(secondEdge.endPoint).length > secondEdge.sourcePoint && adjMatrix(secondEdge.endPoint)(secondEdge.sourcePoint) > 0)) {
              successFlag = false
            }
          }
        }
        if (successFlag) {
          updateRandomMatrix(firstEdge, secondEdge, copyFirstEdge, copySecondEdge, edgeType, randomMatrix)

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
        } else {
          firstEdge.endPoint = copyFirstEdge.endPoint
          secondEdge.endPoint = copySecondEdge.endPoint
          failedAttempts = failedAttempts + 1
        }
      }
    }
  }

  def updateRandomMatrix(firstEdge: Edge, secondEdge: Edge, copyFirstEdge: Edge, copySecondEdge: Edge, edgeType: String, randomMatrix: Array[Array[Int]]): Unit = {
    if (randomMatrix(firstEdge.sourcePoint).length <= firstEdge.endPoint) {
      val arr = new Array[Int](firstEdge.endPoint + 1)
      randomMatrix(firstEdge.sourcePoint).copyToArray(arr, 0, randomMatrix(firstEdge.sourcePoint).length)
      randomMatrix(firstEdge.sourcePoint) = arr
    }
    if (randomMatrix(secondEdge.sourcePoint).length <= secondEdge.endPoint) {
      val arr = new Array[Int](secondEdge.endPoint + 1)
      randomMatrix(secondEdge.sourcePoint).copyToArray(arr, 0, randomMatrix(secondEdge.sourcePoint).length)
      randomMatrix(secondEdge.sourcePoint) = arr
    }
    randomMatrix(firstEdge.sourcePoint)(firstEdge.endPoint) = randomMatrix(copyFirstEdge.sourcePoint)(copyFirstEdge.endPoint)
    randomMatrix(secondEdge.sourcePoint)(secondEdge.endPoint) = randomMatrix(copySecondEdge.sourcePoint)(copySecondEdge.endPoint)

    randomMatrix(copyFirstEdge.sourcePoint)(copyFirstEdge.endPoint) = 0
    randomMatrix(copySecondEdge.sourcePoint)(copySecondEdge.endPoint) = 0

    if (edgeType.compareTo("10") != 0) {
      if (randomMatrix(firstEdge.endPoint).length <= firstEdge.sourcePoint) {
        val arr = new Array[Int](firstEdge.sourcePoint + 1)
        randomMatrix(firstEdge.endPoint).copyToArray(arr, 0, randomMatrix(firstEdge.endPoint).length)
        randomMatrix(firstEdge.endPoint) = arr
      }
      if (randomMatrix(secondEdge.endPoint).length <= secondEdge.sourcePoint) {
        val arr = new Array[Int](secondEdge.sourcePoint + 1)
        randomMatrix(secondEdge.endPoint).copyToArray(arr, 0, randomMatrix(secondEdge.endPoint).length)
        randomMatrix(secondEdge.endPoint) = arr
      }
      randomMatrix(firstEdge.endPoint)(firstEdge.sourcePoint) = randomMatrix(copyFirstEdge.endPoint)(copyFirstEdge.sourcePoint)
      randomMatrix(secondEdge.endPoint)(secondEdge.sourcePoint) = randomMatrix(copySecondEdge.endPoint)(copySecondEdge.sourcePoint)

      randomMatrix(copyFirstEdge.endPoint)(copyFirstEdge.sourcePoint) = 0
      randomMatrix(copySecondEdge.endPoint)(copySecondEdge.sourcePoint) = 0
    }
  }
}
