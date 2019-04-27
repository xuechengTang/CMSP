package cn.Hunan.Method

import cn.Hunan.StructData.Node

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.util.Random


//做一个随机的定点选取。
class CMSP_compute() extends Serializable {

  var a= new ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])]()
  def com(miRnaId: Int, neighbored: Array[ArrayBuffer[Int]], ruleNode: Array[Int], motif_Size: Int): ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])] = {
    val Vext = new ArrayBuffer[Int]()
    val Vopen = new ArrayBuffer[Int]()
    val Vsub = new ArrayBuffer[Int]()

    neighbored(miRnaId).foreach(x => {
      Vext += x
      Vopen += x
    })
    Vsub += miRnaId
    extendSubgraph(Vsub, Vext, Vopen, miRnaId, motif_Size, ruleNode, neighbored)
    a
  }

  def extendSubgraph(Vsub: ArrayBuffer[Int], Vext: ArrayBuffer[Int], Vopen: ArrayBuffer[Int], nodeV: Int, motif_Size: Int, rule_Nodes: Array[Int], neighbored: Array[ArrayBuffer[Int]]): Unit = {
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
          extendSubgraph(newVsub, newVext, newVopen, nodeV, motif_Size, rule_Nodes, neighbored)
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
      /*var i = 0
      while (i < Vext.size) {
        val curNode = Vext(i)
        if (rule_Node(curNode) != freeFlag) {
          Vext.remove(i)
        } else {
          i = i + 1
        }
      }*/
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


}
