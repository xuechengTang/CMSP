package cn.Hunan.StructData

import scala.collection.mutable.ArrayBuffer

class Node(var miRna:Int,var miRna_type:Int,var neighbore:ArrayBuffer[(Int,Int)],var neighbored:ArrayBuffer[(Int,Int)])extends Serializable {

}
object Node{
  def apply( miRna: Int, miRna_type: Int, neighbore: ArrayBuffer[(Int,Int)], neighbored: ArrayBuffer[(Int,Int)]): Node = new Node( miRna, miRna_type, neighbore, neighbored)
}