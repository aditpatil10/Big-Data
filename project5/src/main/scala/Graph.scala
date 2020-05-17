import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import org.apache.spark.rdd.RDD


object Graph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph")                
    val sc = new SparkContext(conf)
    var graph = sc.textFile(args(0)).map(line => {                //read the graph from args(0)
    val vertex = line.split(",")                           
    val group = new Array[Long](vertex.length - 1)
    var i=1;
      while(i <=vertex.length-1){
           group(i-1) = vertex(i).toLong
           i+=1;
          }  
    (vertex(0).toLong, vertex(0).toLong, group) 
       })    
    for (j <- 1 to 5){
    val groupvalue = FlatMapC(graph);
    var mingroup = FindMin(groupvalue);               
    var current=graph.map{case(a) => (a._1, a)}
    var currentt = mingroup.join(current)                  
    val finalg=Fgraph(currentt)                   
      graph=finalg
    }
    val reducedgraph = graph.map(graph => (graph._2, 1)).reduceByKey((x, y) => x + y).sortByKey(true, 0)
    val RedGraphSpaced = reducedgraph.map { case ((k,v)) =>k+" "+v}
    RedGraphSpaced.collect().foreach(println)    
    sc.stop()
  }

 def FlatMapC(GroupVal:RDD[(Long, Long, Array[Long])] ): RDD[(Long,Long)] = {   
    val mapp = GroupVal.flatMap{case(a,b,c) =>
    val len: Int =(c.length) 
    val graphvert = new Array[(Long, Long)](len+1)
    graphvert(0) = (a, b)
    val adjver: Array[Long] = c      
    for (index <- 0 to len-1){
      graphvert(index + 1) = (adjver(index), b)
      }
      graphvert      
      }
   return mapp
 }
 def FindMin(Min:RDD[(Long,Long)]):RDD[(Long,Long)]={
    val mgroup = Min.reduceByKey((a, b) => { 
    var mingroupp: Long =0
    if (a <= b){
      mingroupp = a
      }
      else{
      mingroupp = b
        }
      mingroupp
      })
    return mgroup
 }

def Fgraph(cur:RDD[(Long, (Long, (Long, Long, Array[Long])))] ): RDD[(Long,Long,Array[Long])]={  
    val fgraph=cur.map{case(a,b) => 
    val adjacentt=b._2
    var connected_ver =  (a,b._1,adjacentt._3)       //VID, group, Vertex
        connected_ver
      }
    return fgraph 
}
}