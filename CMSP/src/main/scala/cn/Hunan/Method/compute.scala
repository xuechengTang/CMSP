package cn.Hunan.Method

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class compute() extends Serializable {
  var a = new ArrayBuffer[(ArrayBuffer[Int], ArrayBuffer[Int])]()

  def com(miRnaId: Int, neighbored: Array[ArrayBuffer[Int]], ruleNode: Array[Int], motif_Size: Int, matrix: Array[Array[Int]]): ArrayBuffer[(ArrayBuffer[Int], ArrayBuffer[Int])] = {
    val Vext = new ArrayBuffer[Int]()
    val Vopen = new ArrayBuffer[Int]()
    val Vsub = new ArrayBuffer[Int]()

    Vsub += miRnaId
    /*generateRandomNeighbor(Vsub, neighbored(miRnaId), ruleNode, matrix).foreach(x => {
      Vext += x
      Vopen += x
    })*/
    generateRandomNeighbor(Vsub, neighbored(miRnaId), ruleNode, matrix,neighbored).foreach(x => {
      Vext += x
      Vopen += x
    })

    extendSubgraph(Vsub, Vext, Vopen, miRnaId, motif_Size, ruleNode, neighbored, matrix)
    a
  }

  def extendSubgraph(Vsub: ArrayBuffer[Int], Vext: ArrayBuffer[Int], Vopen: ArrayBuffer[Int], nodeV: Int, motif_Size: Int, rule_Nodes: Array[Int], neighbored: Array[ArrayBuffer[Int]], matrix: Array[Array[Int]]): Unit = {
    while (Vext.nonEmpty) {
      val nodeW = Vext(0)
      Vext.remove(0)

      var freeFlag = -1
      if (Vsub.size == motif_Size - 2) {
        val sat = Array(false, false, false)
        if (Vsub.size > 1) {
          for (i <- Range(1, Vsub.length)) {
            val nodeType = rule_Nodes(Vsub(i))
            sat(nodeType) = true
          }
        }

        val nodeType = rule_Nodes(nodeW)
        sat(nodeType) = true

        if (!sat(1)) {
          freeFlag = 1
        }
        else if (!sat(2)) {
          freeFlag = 2
        }
      }
      var newVext = duplicateArrayList(Vext)
      var newVopen = duplicateArrayList(Vopen)
      newVext = findInNexcl(nodeW, nodeV, Vsub, newVext, newVopen, freeFlag, rule_Nodes, neighbored)
      if (newVext.nonEmpty) {
        var newVsub = duplicateArrayList(Vsub)
        newVsub += nodeW
        /*val num = binarySearch(newVopen,nodeW)
        if(num != -1){
          newVopen.remove(num)
        }*/
        newVopen = newVopen.filter(x => x != nodeW)
        if (newVsub.size == motif_Size - 1) {
          a += ((newVsub, newVext))
          //countSubgraph(newVsub, newVext, neighbor, rule_Nodes)
        } else {
          //extendSubgraph(newVsub, generateRandomNeighbor(Vsub, newVext, rule_Nodes, matrix), newVopen, nodeV, motif_Size, rule_Nodes, neighbored, matrix)
          extendSubgraph(newVsub, generateRandomNeighbor(Vsub, newVext, rule_Nodes, matrix,neighbored), newVopen, nodeV, motif_Size, rule_Nodes, neighbored, matrix)
        }

      }

    }
  }

  def countSubgraph(Vsub: ArrayBuffer[Int], Vext: ArrayBuffer[Int], neighbored: Array[ArrayBuffer[Int]], rule_Nodes: Array[Int]): Unit = {

  }

  def findInNexcl(nodeW: Int, nodeV: Int, Vsub: ArrayBuffer[Int], Vext: ArrayBuffer[Int], Vopen: ArrayBuffer[Int], freeFlag: Int, rule_Node: Array[Int], neighbored: Array[ArrayBuffer[Int]]): ArrayBuffer[Int] = {
    var newVext: ArrayBuffer[Int] = Vext
    if (freeFlag != -1) {
      newVext = newVext.filter(curNode => rule_Node(curNode) == freeFlag)

    }


    neighbored(nodeW).filter(currentPartner => (currentPartner > nodeV) && (!Vsub.contains(currentPartner)) && (!Vopen.contains(currentPartner))).foreach(currentPartner => {
      if (freeFlag == -1 || rule_Node(currentPartner) == freeFlag) {
        newVext += currentPartner
      }
    })
    newVext
  }

  def duplicateArrayList(originalList: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    val newArrayList = new ArrayBuffer[Int]()
    newArrayList ++= originalList
    newArrayList.sorted
  }

  def generateRandomNeighbor(Vsub: ArrayBuffer[Int], arr: ArrayBuffer[Int], ruleNode: Array[Int], adjMatrix: Array[Array[Int]]): ArrayBuffer[Int] = {
    val rd = new Random()
    val tmp = ArrayBuffer[Int]()
    arr.groupBy(start_node => {
      var label = ruleNode(start_node).toString
      for (end_node <- Vsub) {
        if (adjMatrix(start_node).length > end_node) {
          label += adjMatrix(start_node)(end_node)
        } else {
          label += "0"
        }
        if (adjMatrix(end_node).length > start_node) {
          label += adjMatrix(end_node)(start_node)
        } else {
          label += "0"
        }
      }
      label
    }).foreach(x => {
      val length = x._2.length * 0.2
      while (x._2.length > length && length >= 5) {
        val num = rd.nextInt(x._2.length)
        x._2.remove(num)
      }
      while (length <= 5 && x._2.nonEmpty) {
        val num = rd.nextInt(x._2.length)
        x._2.remove(num)
      }
      tmp ++= x._2
    })
    tmp
  }

  def generateRandomNeighbor(Vsub: ArrayBuffer[Int], arr: ArrayBuffer[Int], ruleNode: Array[Int], adjMatrix: Array[Array[Int]],neighbored: Array[ArrayBuffer[Int]]): ArrayBuffer[Int] = {
    //val rd = new Random()
    val tmp = ArrayBuffer[Int]()
    arr.groupBy(start_node => {
      var label = ruleNode(start_node).toString
      for (end_node <- Vsub) {
        if (adjMatrix(start_node).length > end_node) {
          label += adjMatrix(start_node)(end_node)
        } else {
          label += "0"
        }
        if (adjMatrix(end_node).length > start_node) {
          label += adjMatrix(end_node)(start_node)
        } else {
          label += "0"
        }
      }
      label
    }).foreach(x => {
      val length = x._2.length * 0.1
      val arr = x._2.sortBy(y=>neighbored(y).size).reverse
      var num = 0
      while(num <= length){
        tmp += arr(num)
        num = num + 1
      }
    })
    tmp
  }

  def generateRandomNeighbor(arr: ArrayBuffer[Int], ruleNode: Array[Int]): ArrayBuffer[Int] = {
    val rd = new Random()
    //val newArr = new ArrayBuffer[Int]()
    val miRna = arr.filter(x => ruleNode(x) == 0)
    val miRna_length = miRna.length * 0.2
    val TF = arr.filter(x => ruleNode(x) == 1)
    val TF_length = TF.length * 0.2
    val gene = arr.filter(x => ruleNode(x) == 2)
    val gene_length = gene.length * 0.2
    //val arr_Length: Double = arr.length * 0.8
    while (miRna.length > miRna_length) {
      val num = rd.nextInt(miRna.length)
      miRna.remove(num)
    }
    while (TF.length > TF_length) {
      val num = rd.nextInt(TF.length)
      TF.remove(num)
    }
    while (gene.length > gene_length) {
      val num = rd.nextInt(gene.length)
      gene.remove(num)
    }

    miRna ++ TF ++ gene
  }
}
