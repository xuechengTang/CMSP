package cn.Hunan.StructData

class Edge(var sourcePoint:Int,var endPoint:Int) extends Serializable{

}
object Edge{
  def apply(sourcePoint: Int, endPoint: Int): Edge = new Edge(sourcePoint, endPoint)
}