import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io._
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import scala.math._
import scala.collection.mutable.ListBuffer


object Pushkal_Bhatia_task1 {
  
  def main(args: Array[String]) {
    val start = System.nanoTime
    //println("hellllllllllllllllllllllllooooooooooooooooooooooooo")
    val t1 = System.nanoTime
    val txtFile = args(1)
    val txtFile2 = args(0)
    //val txtFile = "testing_small.csv"
    //val txtFile2 = "ratings.csv"
    val ratingfile= ""
    val conf = new SparkConf().setAppName("Pushkal 2nd Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var txtFileLines = sc.textFile(txtFile).cache()   //testing.small
    var txtFileLines2 = sc.textFile(txtFile2).cache() //ratings
    
    var headline = txtFileLines.first
    var txtFilelinesWOhead = txtFileLines.filter(x=>x!=headline)
    var headline2 =txtFileLines2.first
    var txtFilelines2WOhead = txtFileLines2.filter(x=>x!=headline2)
    //txtFilelines2WOhead.take(10).foreach {println}
    //txtFilelinesWOhead.take(10).foreach {println}
    
    //println(txtFilelines2WOhead.count)
    //println(txtFilelinesWOhead.count)
    
    val testfile = txtFilelinesWOhead.flatMap(x => Pushkal1stDataMap(x))
    val ratingsfile = txtFilelines2WOhead.flatMap(y => Pushkal1stDataMap2(y))
   
    //ratingsfile.take(10).foreach(f=>println(f))
    
    val testdatanow = ratingsfile.join(testfile)
    //testdatanow.take(10).foreach{println}
    
    val finaltestdata = testdatanow.flatMap(x=>finaltestmap(x._1._1,x._1._2,x._2._1))
    
    
    
    val final_testData = finaltestdata.map(f=>Rating(f._1._1, f._1._2, f._2))
    val final_rating = ratingsfile.subtractByKey(finaltestdata)
    
    val itemtousers = final_rating.flatMap(x=>itemtousermap(x._1._1, x._1._2,x._2))
    val usertoitems = final_rating.flatMap(x=>itemtousermap(x._1._2, x._1._1,x._2)) //(User1 -> (Item,rating))
    
    val usertoitemlist = usertoitems.groupByKey() //=> user1 : (Item,rating),(Item,rating)
    val itemtouserlist = itemtousers.groupByKey()
    
    val user_and_sumofvalues = usertoitemlist.flatMap(x=>userandsum( x._1,x._2.toList)).collectAsMap() //average user ratings
    val item_and_sumofvalues = itemtouserlist.flatMap(x=>itemandsum( x._1,x._2.toList)).collectAsMap() //Ratings
    
    val ratings = final_rating.map(f=>Rating(f._1._1,f._1._2,f._2))
    val rank = 10
    val numIterations = 10
    //println(ratings.count,"count")
    val model = ALS.train(ratings, rank, numIterations, 0.06)
    val usersProducts = final_testData.map { case Rating(user, product, rate) =>
    (user, product)}
    
    var predictions_previous = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)}
    
    
    
    
    
     def findrating( user:Int, item:Int)  : Map[(Int,Int),Double] ={
      
      var prediction:Double =0.0
      if (user_and_sumofvalues.contains(user) && !item_and_sumofvalues.contains(item))
      prediction = user_and_sumofvalues(user)
      else if(!user_and_sumofvalues.contains(user) && item_and_sumofvalues.contains(item))
        prediction = item_and_sumofvalues(item)
      else
      prediction = 2.5
      
      return Map((user,item)->prediction)
      
    }
    var values_without_predictions = finaltestdata.subtractByKey(predictions_previous).keys.flatMap(f=>findrating(f._1,f._2))
    //println("values_without_predictions: "+values_without_predictions.count())
    val predictions=predictions_previous.union(values_without_predictions)
    //println("predictions: "+predictions.count())
    val ratesAndPreds = final_testData.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)
    //ratesAndPreds.foreach(f=>println(f))
    //var a : RDD[((Int,Int),Double)] = sc.emptyRDD
    //for (x <- values_without_predictions)
      // afindrating(x._1._1,x._1._2)
      
    //var mappartitionbla = values_without_predictions.mapPartitions(partition => findrating(partition).iterator)     
    
    //ratesAndPreds.foreach(f=>println(Math.abs(f._2._1 - f._2._2)))
    
    var zeroto1 = new scala.collection.mutable.ListBuffer[Int]
    var oneto2 = new scala.collection.mutable.ListBuffer[Int]
    var twoto3 = new scala.collection.mutable.ListBuffer[Int]
    var threeto4 = new scala.collection.mutable.ListBuffer[Int]
    var gte4 = new scala.collection.mutable.ListBuffer[Int]
    var count =0
    //println(ratesAndPreds.count)
    var c =ratesAndPreds.count
    for (f <- ratesAndPreds.collect() ){
      
      if ( Math.abs(f._2._1 - f._2._2) >=0 && Math.abs(f._2._1 - f._2._2) < 1 )
       {zeroto1 += 1
       count +=1}
      else if ( Math.abs(f._2._1 - f._2._2) >=1 && Math.abs(f._2._1 - f._2._2) < 2 )
        {oneto2 += 1
        count+=1}
      else if ( Math.abs(f._2._1 - f._2._2) >=2 && Math.abs(f._2._1 - f._2._2) < 3 )
        {twoto3 += 1
        count +=1}
      else if ( Math.abs(f._2._1 - f._2._2) >=3 && Math.abs(f._2._1 - f._2._2) < 4 )
        {threeto4 += 1
        count+=1}
      else
        {gte4 += 1
        count+=1}
        
        
      } 
    
    
    
    //ratings.take(10).foreach{println}
    val usertomovie = txtFileLines.flatMap(x => Pushkal1stDataMap(x))
    //usertomovie.foreach(f=> println(f))
     
    val pw = new PrintWriter(new File("Pushkal_Bhatia_result_task1" ))
    
     var cou = 0 
     pw.write ("UserId," + "MovieId,"+"Pred_Rating"+"\n")
     predictions.toArray().sortBy(f=>(f._1._1,f._1._2)).foreach(f=>
      
      if (cou==0)
      {pw.write(f._1._1.toString() + "," + f._1._2.toString() +"," + f._2.toString())
      cou =1 }
      else
      pw.write("\n"+f._1._1.toString() + "," + f._1._2.toString() +"," + f._2.toString()) )
   
    pw.close
    
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = abs(r1 - r2)
    err * err
    }.mean()
    
    
    //println("Mean Squared Error = " + MSE)
    val rmse = sqrt(MSE)
    
    
     println (">=0 and <1 :" + zeroto1.length)
    println (">=1 and <2: " + oneto2.length)
      println (">=2 and <3: " + twoto3.length)
      println (">=3 and <4: " + threeto4.length)
      println (">=4: " + gte4.length)
    println("RMSE = " + rmse)
   
    val end = (System.nanoTime -start) / 1e9d
    println("The total execution time taken is " + end + " sec")
}
  
  
  
    def userandsum(d1:Int,d2:List[(Int,Double)]) : Map[Int,Double] = {
    var datamap = Map[Int,Double]()
    var sum = 0.0
    for (x <- d2) {
      
      sum = sum + x._2
    }
    var avg = sum/d2.length
    datamap = Map(d1 -> avg)
    return datamap
     
    
   }
   
    
     def itemandsum(d1:Int,d2:List[(Int,Double)]) : Map[Int,Double] = {
    var datamap = Map[Int,Double]()
    var sum = 0.0
    for (x <- d2) {
      
      sum = sum + x._2
    }
    var avg = sum/d2.length
    datamap = Map(d1 -> avg)
    return datamap
   }
    
  
   def itemtousermap(d1:Int,d2:Int,d3:Double): Map[Int,(Int,Double)] ={
     
     var datamap = Map[Int,(Int,Double)]()
     datamap = Map(d2.toInt -> (d1.toInt -> d3.toDouble))
     
     return datamap
   }
  
  
   def Pushkal1stDataMap2(data:String): Map[(Int,Int),Double] = {
     val array = data.split(",")
     var dataMap = Map[(Int,Int),Double]()
     dataMap=Map((array(0).toInt -> array(1).toInt) -> array(2).toDouble)
    
    return dataMap
    } 
  
   
   def Pushkal1stDataMap(data:String): Map[(Int,Int),Double] = {
     val array = data.split(",")
     var dataMap = Map[(Int,Int),Double]()
     dataMap=Map((array(0).toInt -> array(1).toInt) -> 1.00)
    
    return dataMap
    } 
   
    def finaltestmap(data1:Int, data2:Int, data3:Double): Map[(Int,Int),Double] = {
     var dataMap = Map[(Int,Int),Double]()
     dataMap=Map((data1.toInt -> data2.toInt) -> data3.toDouble)
    
    return dataMap
    } 
  
  
}