import org.apache.spark.graphx.{Graph=>Graphh, VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object GraphComponents {
  def main ( args: Array[String] ) {
  val conf = new SparkConf().setAppName("Graph")
  val sc = new SparkContext(conf)
  val inputval =sc.textFile(args(0)).map(line => {  val (gvert,adjgroup)=line.split(",").splitAt(1)
  (gvert(0).toLong,adjgroup.toList.map(_.toLong))})

  val flatgraph= inputval.flatMap(a=> a._2.map(b=>(a._1,b)))
  val EdgeMapp= flatgraph.map(gnode=>Edge(gnode._1,gnode._2,gnode._1))                                        
                                         
  val Cregraph: Graphh[Long,Long]=graphcreation(EdgeMapp)

  val connect=Cregraph.pregel(Long.MaxValue,5)((id,Prevgroup,newGroup)=> math.min(Prevgroup,newGroup),
    triplet=>{
        if(triplet.attr<triplet.dstAttr){
         Iterator((triplet.dstId,triplet.attr)) }
            else if((triplet.srcAttr<triplet.attr)){
              Iterator((triplet.dstId,triplet.srcAttr)) }
            else{
              Iterator.empty }          
    	    }, (c1,c2)=>math.min(c1,c2))
	val res = connect.vertices
	val countvertex= res.map(Cregraph=>(Cregraph._2,1))
    val reducedvertices=countvertex.reduceByKey(_ + _).sortByKey()

	val finalgraph= reducedvertices.map(keycom=>keycom._1.toString+" "+keycom._2.toString )
	finalgraph.collect().foreach(println)
                                    
  }

  def graphcreation(Edges:RDD[Edge[Long]]):Graphh[VertexId,Long]={   
  val interGraph= Graphh.fromEdges(Edges,"defaultProperty").mapVertices((id,_)=>id)
   return interGraph
  }
}
