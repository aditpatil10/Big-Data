import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Histogram")
    val sc = new SparkContext(conf)
    val color = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                               (((1, a(0)), 1), ((2, a(1)), 1), ((3, a(2)), 1))     } ) 
    /*
    val red = color._1.reduceByKey((x, y) => x + y)
    val green = color._2.reduceByKey((x, y) => x + y)
    val blue = color._3.reduceByKey((x, y) => x + y)
    val total = (red, green blue)
    */
    val red = color.map ( x => { x._1 })
    val green = color.map ( x => { x._2 })
    val blue = color.map ( x => { x._3 })

    val redd = red.reduceByKey(_+_)
    val greenn = green.reduceByKey(_+_)
    val bluee = blue.reduceByKey(_+_)

    val total = redd.union(greenn).union(bluee)
    val totall = total.map { case ((a,b), c) => a + "\t" + b + "\t" + c }
    totall.collect().foreach(println)
    sc.stop()     
                                                                            
    /* ... */
  }
}

 /*
  val red = color.map( r => ((1, a(0)),1) ).reduceByKey((x, y) => x + y)
    val green = color.map( g => ((2, a(1)),1) ).reduceByKey((x, y) => x + y)
    val blue = color.map( b => ((3, a(2)),1) ).reduceByKey((x, y) => x + y)
    val 
    res.saveAsTextFile(args(2))                                                                                 
    /* ... */
    */