import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import scala.math._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.HashMap



object Pushkal_Bhatia_task2 {
  
  def main(args: Array[String]) {
    val start = System.nanoTime
    var mp = scala.collection.immutable.Map[(Int,Int),Int]()
    val txtFile = args(1)
    //val txtFile = "testing_small.csv"
    //val txtFile2 = "ratings.csv"
    val txtFile2 = args(0)
    //val ratingfile= ""
    val conf = new SparkConf().setAppName("Pushkal 2nd Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var txtFileLines = sc.textFile(txtFile,4).cache()   //testing.small
    var txtFileLines2 = sc.textFile(txtFile2,4).cache() //ratings
    
    var headline = txtFileLines.first
    var txtFilelinesWOhead = txtFileLines.filter(x=>x!=headline)
    var headline2 =txtFileLines2.first
    var txtFilelines2WOhead = txtFileLines2.filter(x=>x!=headline2)
    //txtFilelines2WOhead.take(10).foreach {println}
    //txtFilelinesWOhead.foreach {println}
    
    //println(txtFilelines2WOhead.count)
    //println(txtFilelinesWOhead.count)
    
    val testfile = txtFilelinesWOhead.flatMap(x => Pushkal1stDataMap(x))
    val ratingsfile = txtFilelines2WOhead.flatMap(y => Pushkal1stDataMap2(y))
   
    //ratingsfile.take(10).foreach(f=>println(f))
    
    val testdatanow = ratingsfile.join(testfile)
    
    val finaltestdata = testdatanow.flatMap(x=>finaltestmap(x._1._1,x._1._2,x._2._1))
    
    
    
    //val final_testData = finaltestdata.map(f=>Rating(f._1._1, f._1._2, f._2))
    val final_rating = ratingsfile.subtractByKey(finaltestdata)
    //var final_rating_gg = final_rating.map(f=> Map((f._1._1 -> f._1._2) -> f._2))
    
    
    val itemtousers = final_rating.flatMap(x=>itemtousermap(x._1._1, x._1._2,x._2))
    val usertoitems = final_rating.flatMap(x=>itemtousermap(x._1._2, x._1._1,x._2)) //(User1 -> (Item,rating))
    
    //itemtousers.foreach(println)
    val itemtouserlist = itemtousers.groupByKey()
    val usertoitemlist = usertoitems.groupByKey() //=> user1 : (Item,rating),(Item,rating)
    //println(itemtouserlist.count())
    
    val item_and_sumofvalues = itemtouserlist.flatMap(x=>itemandsum( x._1,x._2.toList)) //Ratings
    val item_and_sumofvaluesmap =item_and_sumofvalues.collectAsMap()
    val user_and_sumofvalues = usertoitemlist.flatMap(x=>userandsum( x._1,x._2.toList)).collectAsMap() //average user ratings
    //user_and_sumofvalues.foreach{println}
    
    val forpredmap = itemtousers.map(f=> (f._1,f._2._1)->f._2._2).collectAsMap() //contains Map((item,user),rating)
    //println(forpredmap.size)
    
    //item_and_sumofvalues.foreach(println)
    
    
    val items_rating_and_averagejoined = itemtouserlist.join(item_and_sumofvalues) //average and item
    //items_rating_and_averagejoined.foreach(println)
    
    
    //items with their users and average difference
    var item_with_rating_minus_avg = items_rating_and_averagejoined.flatMap(x=>mapfordifference(x._1,x._2._1.toList,x._2._2)).collectAsMap()   
    //item_with_rating_minus_avg.foreach(println) 
    //println(item_with_rating_minus_avg.count())
    
    
    var lisoftest=new scala.collection.mutable.ListBuffer[((Int,Int),Double)]  
    //pearsoncoff(118985,624)
     
    def calculate(x :Iterator[((Int, Int), Double)]):ListBuffer[((Int,Int),Double)]={
     
     //println("Laaawdddaaaa")
     for (arg <- x){
       lisoftest += pearsoncoff(arg._1._2,arg._1._1)
     }
     
     //println(lisoftest)
     return lisoftest
     
    }
    
      
      
    def pearsoncoff(item:Int,user:Int): ((Int,Int),Double)={
      
      //if (user == 673)
       // println("found user 673")
      
      var coeffmaps = scala.collection.immutable.Map[(Int,Int),Double]()
      var mapforpred = new ListBuffer[((Int,Int),Double)]() //for itm_usr_wcoeff
      var list2 = scala.collection.immutable.Map[Int,Double]() //will contain original ratings for denominator
      var prediction = 0.0
      var prednum = 0.0
      var predden = 0.0
      var neigh = 20
      var flag = 100
      if (item_with_rating_minus_avg.contains(item) ){
        flag = 0
        for (x <- item_with_rating_minus_avg(item))
          list2 += (x._1 -> x._2)
      //}
          
      //println(item, item_with_rating_minus_avg.lookup(item))
      //println(list2, "list2----------------------------")
      
     // if (item_with_rating_minus_avg)
      for (ke <- item_with_rating_minus_avg){
        if (ke._1 != item){//if_1
        var wcoeff = 0.0
        var r1=0.0
        var r2=0.0
        var total =0
        var numerator = 0.0
        var list1 = scala.collection.immutable.Map[Int,Double]() //will contain average ratings for numerator
        
        
        if (ke._2.contains(user)){ //ke._2 contains all the values,and we check if this user is in those values
          flag=1
          for (l1 <- ke._2)
            list1 += (l1._1 -> l1._2) //vector for list1
            

        
        //if (!coeffmaps.contains(q)){
        
          for (v1 <- list1){
          if (list2.contains(v1._1)){
            numerator += (list2(v1._1)*list1(v1._1))
            //r1 += (list1(v1._1)*list1(v1._1))
            //r2 += (list2(v1._1)*list2(v1._1))
        }}
          
          for (v1 <- list1)
            r1 += (list1(v1._1)*list1(v1._1))
            
          for (v2 <- list2)
            r2 += (list2(v2._1)*list2(v2._1))
            
          
          //println(numerator,r1,r2,"**********************************************")
          if (r1 == 0 || r2 == 0)
            wcoeff = 0.0000001
            //println ("coefficient is 0", user, item)}
          else
            wcoeff = numerator/(sqrt(r1*r2))
            
            wcoeff = wcoeff * pow(abs(wcoeff),1.5)
            coeffmaps += ((item,ke._1)->wcoeff)
            
            
            
          //println(q,wcoeff)
          
        //}
          
        
        }// if_2
        }//if_1
          
       
        
      }//for ends
              
      }  
    
      
    if (flag == 0)
    {
      prediction = item_and_sumofvaluesmap(item)
    }
    else
    {
    //println(coeffmaps.size,"================================")
    var coeffmapprediction = coeffmaps.toSeq.sortBy(_._2).reverse
    coeffmapprediction.take(neigh).foreach{ x=>
      prednum += forpredmap(x._1._2,user)*(x._2) 
      predden += abs(x._2)
      }
    
    
    if (predden == 0.0)//For NaN items
    { if (user_and_sumofvalues.contains(user))
      prediction = user_and_sumofvalues(user)
      
    else
      prediction = 2.5
      item_with_rating_minus_avg.updated(item, (user -> prediction))
    }
    else
    prediction = prednum/predden
    }
    
    
    if (prediction < 0.0 || prediction > 5)
      prediction = item_and_sumofvaluesmap(item)
    //var mapforpred2 = mapforpred.collect()
    
      
      return (((user,item), prediction))
    }//function ends
    
    
  var mappartitionbla = finaltestdata.mapPartitions(partition => calculate(partition).iterator)  
  //mappartitionbla.foreach{println} 
  //To calculate RMSE
  
  var predicted_and_actualvalue = mappartitionbla.join(finaltestdata)
  //predicted_and_actualvalue.foreach(f=>println(f))
  //println(predicted_and_actualvalue.count())
  
  
  var zeroto1 = new scala.collection.mutable.ListBuffer[Int]
    var oneto2 = new scala.collection.mutable.ListBuffer[Int]
    var twoto3 = new scala.collection.mutable.ListBuffer[Int]
    var threeto4 = new scala.collection.mutable.ListBuffer[Int]
    var gte4 = new scala.collection.mutable.ListBuffer[Int]
    var count =0
    var c =predicted_and_actualvalue.count
    for (f <- predicted_and_actualvalue.collect() ){
      
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
  
  
  val usertomovie = txtFileLines.flatMap(x => Pushkal1stDataMap(x))
    //usertomovie.foreach(f=> println(f))
     
    val pw = new PrintWriter(new File("Pushkal_Bhatia_result_task2" ))
    
     var cou = 0 
     pw.write ("UserId," + "MovieId,"+"Pred_Rating"+"\n")
     predicted_and_actualvalue.toArray().sortBy(f=>(f._1._1,f._1._2)).foreach(f=>
      
      if (cou==0)
      {pw.write(f._1._1.toString() + "," + f._1._2.toString() +"," + f._2._1.toString())
      cou =1 }
      else
      pw.write("\n"+f._1._1.toString() + "," + f._1._2.toString() +"," + f._2._1.toString()) )
   
    pw.close
  
  
  val MSE = predicted_and_actualvalue.map { case ((user, product), (r1, r2)) =>
    val err = abs(r1 - r2)
    err * err
    }.mean()
    
    
    //println("Mean Squared Error = " + MSE)
    val rmse = sqrt(MSE)
    
    
     println (">=0 and <1: " + zeroto1.length)
    println (">=1 and <2: " + oneto2.length)
      println (">=2 and <3: " + twoto3.length)
      println (">=3 and <4: " + threeto4.length)
      println (">=4: " + gte4.length)
    println("RMSE = " + rmse)
  
  
  
  //println(mappartitionbla.count())
  val end = (System.nanoTime -start) / 1e9d
  //println(end)
  }
  
  
  
  
  
//-----------------------------------------------------------------------------------------------------------------------------------
  
  
   def itemtousermap(d1:Int,d2:Int,d3:Double): Map[Int,(Int,Double)] ={
     
     var datamap = Map[Int,(Int,Double)]()
     datamap = Map(d2.toInt -> (d1.toInt -> d3.toDouble))
     
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
   
   def mapfordifference(d1:Int,d2:List[(Int,Double)],d3:Double) : Map[Int,Map[Int,Double]] ={
     var datamap = Map[Int,Map[Int,Double]]()
     var lis =Map[Int,Double]()
     for (x <- d2){
       var z = x._2-d3
       lis += ((x._1 -> z))
       
     }
     datamap = Map(d1 -> lis)
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