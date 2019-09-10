import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import scala.math._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.HashMap
import scala.collection.mutable.PriorityQueue



object Bhatia_Pushkal_clustering {
  
  def main(args: Array[String]) {
    var mp = scala.collection.immutable.Map[(Int,Int),Int]()
    //val txtFile = args(1)
    val txtFile = args(0)
    val conf = new SparkConf().setAppName("Pushkal 2nd Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var filetouse = sc.textFile(txtFile,4).cache()   //filetouse
    
    val original =  filetouse.flatMap(x => Pushkal1stDataMap(x)).collectAsMap()
    val testfile = filetouse.flatMap(x => Pushkal1stDataMap(x)).groupByKey() //set X of object
    //testfile.foreach(println)
    
    val testfiletomap =filetouse.flatMap(x => Pushkal1stDataMap(x)).groupByKey().collectAsMap()
    var lis = scala.collection.mutable.ListBuffer[Set[(Float,Float,Float,Float)]]()
    var listmapduo = scala.collection.mutable.Map[Set[(Float,Float,Float,Float)],Int]()
    
    for (x <- testfile.collect){
      if (x._2.size > 1){
        lis += Set(x._1)
        listmapduo(Set(x._1)) = x._2.size-1

      }
    }
    
    
    
    //println ("list------")
    //lis.foreach(println)
    //listmapduo.foreach(println)
    
    
    var points = scala.collection.mutable.Set(testfile.keys.collect():_*)
    //points.foreach(println)
    
    
    
    var k = args(1).toInt   
    //cluster
    var c = scala.collection.mutable.Map[(Float,Float,Float,Float),Set[(Float,Float,Float,Float)]]() //List of initial centroids. Each point taken as a centroid
    
    for (i <- points)
      c += ((i,scala.collection.immutable.Set(i)))

    var distancemap = scala.collection.mutable.Map[((Float,Float,Float,Float),(Float,Float,Float,Float)),Float]()
    def diff(t2: (((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)) = t2._2
    var prq = new PriorityQueue[(((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)]()(Ordering.by(diff).reverse)
    
    
    def calculatedistance(d1:(Float,Float,Float,Float), d2:(Float,Float,Float,Float)) : (((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)={
      
      var distance : Float = 0
      distance = sqrt(pow((d1._1-d2._1),2).toFloat + pow((d1._2- d2._2),2).toFloat + pow((d1._3- d2._3),2).toFloat+pow((d1._4- d2._4),2).toFloat ).toFloat
      
      return ((d1,d2),distance)
    }
    
    var z= testfile.keys.collect
    
    for (i <- 0 to z.size-2){
      for (j <- i+1 to z.size-1){
        prq += calculatedistance(z(i),z(j))
        distancemap += calculatedistance(z(i),z(j))
        }
    }
    
    //distancemap.foreach(println) 
   
    
    var discarded_set = scala.collection.mutable.Set[(Float,Float,Float,Float)]()
    
    var i=0
    //println("le bhnchod infinite loop1")
    //c.size > k
    while (c.size > k){
      
      
      var deq = prq.dequeue()
      
      //var variable = 0
      while (!scala.collection.mutable.Set(deq._1._1).intersect(discarded_set).isEmpty || !scala.collection.mutable.Set(deq._1._2).intersect(discarded_set).isEmpty){
        deq = prq.dequeue()
      }
      
      
      discarded_set += deq._1._1
      discarded_set += deq._1._2

      
      //println ("here?")
      c(deq._1._2) = c(deq._1._2).union(c(deq._1._1))                                     //Added the values to the cluster
      
      c.remove(deq._1._1)                                                  //Deleted the added point
      //println ("here?1")
      var clus = c(deq._1._2)
      
      var centroid : (Float,Float,Float,Float) = (0,0,0,0)
      
      var sum1 :Float = 0
      var sum2 :Float = 0
      var sum3 :Float = 0
      var sum4 :Float = 0
      for (x <- clus){
       
        sum1 += x._1
        sum2 += x._2
        sum3 += x._3
        sum4 += x._4
      }
      
      sum1 = sum1/clus.size
      sum2 = sum2/clus.size
      sum3 = sum3/clus.size
      sum4 = sum4/clus.size
      
      centroid = (sum1,sum2,sum3,sum4)
                                      
      c.remove(deq._1._2)                                      //removing old cluster from c
      //println ("here?2")
                                       // And adding new cluster, with the centroid   
      
      for (a <- c.keys ){
        prq += calculatedistance(a,centroid)       //Calculating distances for points with new centroid
      }
      c(centroid) = clus 
      
      //points += centroid                           //Added new point(centroid) to points
      //c.foreach(println)
      
    }
    
    var listoflist = scala.collection.mutable.ListBuffer[List[(Float,Float,Float,Float)]]()
    for (elem <- c.values){
      listoflist += elem.toList
    }
    
    //println("le bhnchod infinite loop2")
    var cluslist =  scala.collection.mutable.Map[(Float,Float,Float,Float),scala.collection.mutable.ListBuffer[(Float,Float,Float,Float)]]()
    for (ii <- c)
      cluslist(ii._1) = ii._2.to[ListBuffer]
    
    
    
    for (ii <- c){
      for (jj <- lis){
        if (!c(ii._1).intersect(jj).isEmpty){
         for (el <- 0 until listmapduo(jj)){
           cluslist(ii._1) += jj.toList(0)
         }
        }
      }
    }
    
    
    //cluslist.foreach(f => println(f._2,f._2.size))
    
    //Naming clusters
    
    var wrongones = 0
    
    
    
    //var finalmap = scala.collection.mutable.Map[(String),scala.collection.mutable.ListBuffer[(Float,Float,Float,Float)]]()
    
    val pw = new PrintWriter(new File("Pushkal_Bhatia_"+ k.toString() ))
    
    for (a <- cluslist){
      var finalmap = scala.collection.mutable.Map[(String),scala.collection.mutable.ListBuffer[(Float,Float,Float,Float)]]()
      var name = scala.collection.mutable.Map[(String),(Int)]()
      for (z <- a._2){
        if (!name.contains(original(z)))
          name(original(z))=1
        else
          name(original(z))+= 1
      }
      var stri = name.maxBy(f => f._2)
      finalmap(stri._1) = a._2
      
      
      for (x <- finalmap(stri._1)){
        if (original(x) != (stri._1) ){
          finalmap(stri._1) -= x 
          if (!finalmap.contains(original(x)))
            finalmap(original(x)) = ListBuffer(x)
          else
            finalmap(original(x)) += x
            wrongones += 1}
      }
      
      //println("loop")
      //finalmap.foreach(println)
      
      
      pw.write("Cluster: "+ stri._1 + "\n")
      for (y <-finalmap ){
        var z= finalmap(y._1)
        for (x <- z)
        pw.write("["+ x._1+ ", " + x._2 + ", " + x._3 + ", " + x._4+", " + "'"+y._1+"'"+"]"+"\n")
      }
      var finalc = 0
      for (y <- finalmap.values)
        for (c <- y)
          finalc += 1
        
      pw.write("Number of points in this cluster:"+finalc+"\n")
      pw.write("\n")
      
      
    }
    
    
  
    
    pw.write("Number of points wrongly assigned:"+wrongones)
     
    //finalmap.foreach(println)
    pw.close
    
  }
  
  
  def Pushkal1stDataMap(data:String): Map[(Float,Float,Float,Float),String] = {
   val array = data.split(",")
   var dataMap = Map[(Float,Float,Float,Float),String]()
   dataMap=Map(((array(0).toFloat,array(1).toFloat,array(2).toFloat,array(3).toFloat) -> array(4) ))
  
  return dataMap
  } 
   

}