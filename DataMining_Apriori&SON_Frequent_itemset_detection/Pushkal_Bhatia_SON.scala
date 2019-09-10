import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.collection.immutable.HashMap
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import scala.collection.generic.Sorted
import scala.collection.mutable.SortedSet
import sun.security.util.Length
import scala.util.control.Breaks._





object Pushkal_Bhatia_SON {
  
  def main(args: Array[String]) {
    val txtFile = args(1) //users.dat
    val txtFile2 = args(2) //ratings.dat
    var support = args(3).toInt
    val support1 =support
    var finalfrequent = new ListBuffer[List[Int]]()
    var casenum = args(0).toInt
   
    
    val t1 = System.nanoTime
    //val txtFile2ndpart = "/home/pushkal/SampleApp/src/main/scala/movies.dat"
    val conf = new SparkConf().setAppName("Pushkal 2nd Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val txtFileLines = sc.textFile(txtFile ,4).cache()
    val txtFileLines2 = sc.textFile(txtFile2 ,4).cache()
    //val txtFileLines3= sc.textFile(txtFile2ndpart , 2).cache()
    val maleuser = txtFileLines.flatMap(x => Pushkal1stDataMap(x))
    val femaleuser = txtFileLines.flatMap(x => Pushkal1stDataMapForFemale(x))//---------------------------\//malefemale.foreach(f=>println(f._1))
    val ratings = txtFileLines2.flatMap(y => Pushkal2ndDataMap(y))
    //ratings.foreach(f=>println(f))
    
    
    //This joins ratings and malefemale to gve the joined result
    val joinedvalues = ratings.join(maleuser)
    val joinedvaluesforFemale= ratings.join(femaleuser) //-----------------------
    //joinedvaluesforFemale.foreach {println} //--------------------------
    
    //For case1------------------------------------------------------------------------------
    
    if (casenum == 1){
      val returnlist = joinedvalues.flatMap(t => Pushkal3rdDataMap (t._1, t._2._1))
     val movie_gourp_for_user=returnlist.groupByKey.mapValues(_.toSet[Int]) 
     
     val sup=support/movie_gourp_for_user.getNumPartitions
    
    val mapped = movie_gourp_for_user.mapPartitions{
                        (iterator) => {
                         Apriori(iterator,sup).iterator
                         
                        }
                     }
    var firstmapred= mapped.reduceByKey(_+_).keys.distinct()
    //firstmapred.foreach {println}
    
    val item=firstmapred.collect()
    val mapped2 = movie_gourp_for_user.mapPartitions{
                        (iterator) => {
                         countintotal(iterator,item,support).iterator
                         
                        }
                     }
    //mapped2.foreach{println}
    var secondmapred=mapped2.reduceByKey(_+_)
    //secondmapred.foreach(f=>println(f))
    
    
    
    for (x <- secondmapred.collect()){
      if (x._2 >= support1){
        finalfrequent += x._1.toList.sortWith(_<_)
      }
    }
    
    
    
    
    val output=finalfrequent.toList.sortBy(f=>f(0)).sortBy(f => f.size)
    // val output=finalfrequent.toList.sortBy(f=>(f(0))).sortBy(f => f.size)
    //output.foreach(f=>println(f))
    
    
    
    /*var outputlist = new ListBuffer[List[Int]]()
    var ite =0
    while (ite < output.length){
      
      var flag=0
      if (outputlist.isEmpty)
      { outputlist += output(ite) 
         }
      
      else if (output(ite-1).length < output(ite).length ){
        println("prev",output(ite-1),"current",output(ite),"1")
        outputlist += output(ite)
      }
      
      else if (output(ite-1).length == output(ite).length  && output(ite-1)(0) != output(ite)(0)){
        println("prev",output(ite-1),"current",output(ite),"2",output(ite)(0),output(ite-1)(0))
        outputlist += output(ite)
      }
      
      else if (output(ite-1)(0) == output(ite)(0) &&  output(ite-1).length == output(ite).length ){
        println("prev",output(ite-1),"current",output(ite),"3")
        breakable{for (ii <- 1 until output(ite).length){
          println ("ii -->",output(ite)(ii), "ii-1-->", output(ite-1)(ii))
            if (output(ite)(ii) > output(ite-1)(ii)){
              outputlist += output(ite) 
              flag=1
              break}
            else if (output(ite)(ii) < output(ite-1)(ii)){
              print ("here")
              var tempo = output(ite-1)
              outputlist((outputlist.length)-1) = output(ite)
              outputlist += tempo
              flag=1
              
              break 
            }}}
      }
     else { outputlist += output(ite)  
     println("prev",output(ite-1),"current",output(ite),"last")}
     ite +=1 
    }*/
    
    var c = 1
	  var finallist = scala.collection.mutable.HashMap[Int,List[String]]()
        for (i<-finalfrequent.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
          if (i.size==c){
	          //var tempoworkingstring = ("("+i.mkString(",")+")")
            var tempoworkingstring = ""
              for (xan<-i){
               for (y <-0 to (5-xan.toString.length())){
                 tempoworkingstring += "0"  
               }
               tempoworkingstring += xan.toString +","
              }
              tempoworkingstring = tempoworkingstring.dropRight(1)
	          var name = finallist.getOrElse(c,List[String]())
	          name = tempoworkingstring :: name
	          finallist.update(c, name)
          }
          else if(i.size==c+1){	        
	           var tempoworkingstring = ""
              for (xan<-i){
               for (y <-0 to (5-xan.toString.length())){
                 tempoworkingstring += "0"  
               }
               tempoworkingstring += xan.toString +","
              }
              tempoworkingstring = tempoworkingstring.dropRight(1)
	           var name = finallist.getOrElse(c+1,List[String]()) 
	          name = tempoworkingstring :: name
	          finallist.update(c+1, name)
	          c+=1}}
         
    var mapfinale = scala.collection.mutable.HashMap[Int,List[String]]() 
    for (dd <- finallist.keys) {
      var temp = List[String]()
      temp = finallist(dd)
      mapfinale.update(dd, temp.sortWith(getSorted))
    }
    var stri ="" 
    var finallyfinal = new ListBuffer[List[Int]]()
    for (zoo1 <- mapfinale(1)){
        stri += "(" + zoo1.toInt + ")" + "," +" "
        }
    stri=stri.dropRight(2)
    stri += "\n"
     
    
    for (zoo <- 2 to mapfinale.size){
      var x = mapfinale(zoo)
      for (ic <- x){
        var li= ic.split(",")
        stri += "(" 
        for (cz <- li){
         stri += cz.toInt + ","
        }
        stri=stri.dropRight(1)
        stri += ")" + "," + " "
        
        
      }
      stri=stri.dropRight(2)
      stri += "\n"
    }
    stri=stri.dropRight(1)
    ////println(stri)
    //finallyfinal.foreach(println)
    //println(mapfinale.size)    
        
    
    
    val pw = new PrintWriter(new File("Pushkal_Bhatia_SON.case1_"+support+".txt" ))
    pw.write(stri)
    pw.close
    
    /*var cooo=1
    for (x <- output){
      if (x.size == cooo )
         print("(" + x.mkString(",") + ")")
      else if (x.size > cooo){
        println("(" + x.mkString(",") + ")")
        c+=1
      }
    }*/
    
    
    
    
    //val duration = (System.nanoTime - t1) / 1e9d
    //println (duration)
     
    }
    
    
    //For case 2-------------------------------------------------------------------
    
    if (casenum == 2){
      val returnlistFor2nd = joinedvaluesforFemale.flatMap(t => Pushkal3rdDataMap (t._2._1,t._1))
    //returnlistFor2nd.foreach {println}
      val usergroupformovie = returnlistFor2nd.groupByKey.mapValues(_.toSet[Int])
      /*val usergroupformovie = returnlistFor2nd.aggregateByKey(Scala.collection.mutable.Set[Int])(
        (numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1 ++= numList2; numList1})
.mapValues(_.toSet[Int])*/
      
      
      val sup=support/usergroupformovie.getNumPartitions
    
    val mapped = usergroupformovie.mapPartitions{
                        (iterator) => {
                         Apriori(iterator,sup).iterator
                         
                        }
                     }
    var firstmapred= mapped.reduceByKey(_+_).keys.distinct()
    //firstmapred.foreach {println}
    
    val item=firstmapred.collect()
    val mapped2 = usergroupformovie.mapPartitions{
                        (iterator) => {
                         countintotal(iterator,item,support).iterator
                         
                        }
                     }
    //mapped2.foreach{println}
    var secondmapred=mapped2.reduceByKey(_+_)
    //secondmapred.foreach(f=>println(f))
    
    
    
    for (x <- secondmapred.collect()){
      if (x._2 >= support1){
        finalfrequent += x._1.toList.sortWith(_<_)
      }
    }
    
    
    /*val output=finalfrequent.toList.sortBy(f=>f(0)).sortBy(f => f.size)
    // val output=finalfrequent.toList.sortBy(f=>(f(0))).sortBy(f => f.size)
    //output.foreach(f=>println(f))
    var c=1
    for (x <- output){
      if (x.size == c )
         print("(" + x.mkString(",") + ")")
      else if (x.size > c){
        println("(" + x.mkString(",") + ")")
        c+=1
      }
    }*/
    
    
     var c = 1
	  var finallist = scala.collection.mutable.HashMap[Int,List[String]]()
        for (i<-finalfrequent.sortBy(f=>f.sum).sortBy(f=>f(0)).sortBy(f=>f.size)){
          if (i.size==c){
	          //var tempoworkingstring = ("("+i.mkString(",")+")")
            var tempoworkingstring = ""
              for (xan<-i){
               for (y <-0 to (5-xan.toString.length())){
                 tempoworkingstring += "0"  
               }
               tempoworkingstring += xan.toString +","
              }
              tempoworkingstring = tempoworkingstring.dropRight(1)
	          var name = finallist.getOrElse(c,List[String]())
	          name = tempoworkingstring :: name
	          finallist.update(c, name)
          }
          else if(i.size==c+1){	        
	           var tempoworkingstring = ""
              for (xan<-i){
               for (y <-0 to (5-xan.toString.length())){
                 tempoworkingstring += "0"  
               }
               tempoworkingstring += xan.toString +","
              }
              tempoworkingstring = tempoworkingstring.dropRight(1)
	           var name = finallist.getOrElse(c+1,List[String]()) 
	          name = tempoworkingstring :: name
	          finallist.update(c+1, name)
	          c+=1}}
         
    var mapfinale = scala.collection.mutable.HashMap[Int,List[String]]() 
    for (dd <- finallist.keys) {
      var temp = List[String]()
      temp = finallist(dd)
      mapfinale.update(dd, temp.sortWith(getSorted))
    }
    var stri ="" 
    var finallyfinal = new ListBuffer[List[Int]]()
    for (zoo1 <- mapfinale(1)){
        stri += "(" + zoo1.toInt + ")" + "," +" "
        }
    stri=stri.dropRight(2)
    stri += "\n"
     
    
    for (zoo <- 2 to mapfinale.size){
      var x = mapfinale(zoo)
      for (ic <- x){
        var li= ic.split(",")
        stri += "(" 
        for (cz <- li){
         stri += cz.toInt + ","
        }
        stri=stri.dropRight(1)
        stri += ")" + "," + " "
        
        
      }
      stri=stri.dropRight(2)
      stri += "\n"
    }
    stri=stri.dropRight(1)
    println(stri)
    //finallyfinal.foreach(println)
    //println(mapfinale.size)   
    
    
    val pw = new PrintWriter(new File("Pushkal_Bhatia_SON.case2_"+support+".txt" ))
    pw.write(stri)
    pw.close
    
    
    
    
    
    
    //val duration = (System.nanoTime - t1) / 1e9d
    //println (duration)
      
      
    }
    
    
    
    
    
    
    
    
    //new PrintWriter("Pushkal_Bhatia_result.txt") { write(finalmap.toList.sortBy(q=>(q._1._1.toDouble ,q._1 ._2)).foreach(f=>println(f._1._1 +","+f._1 ._2 +","+f._2)).toString()); close }

    
  }
   
  
  def getSorted(s:String, c:String) = {s<c}
  
  def countintotal(userdata : Iterator[(Int,Set[Int])],firstmapred: Array[Set[Int]],support:Float ): HashMap[Set[Int],Int]={ 
      
    var finallist = HashMap[Set[Int],Int]()
    
    
    for (x <- userdata){
      for (y <- firstmapred){       
        if (x._2.intersect(y) == y){
          var s=finallist.getOrElse(y, 0) + 1
         finallist += y -> s
        }
      }
    }
    
    return finallist
    
  }
  
  
     
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Creation of datanmaps that map data taking in arguments(mapped as key value pairs)
  
   def Pushkal1stDataMap(data:String): Map[Int,String] = {
     val array = data.split("::")
     var dataMap = Map[Int,String]()
     if (array(1)=="M"){
     dataMap=Map(array(0).toInt -> array(1))
    }
    return dataMap
    }  
   
   
   def Pushkal1stDataMapForFemale(data:String): Map[Int,String] = {
     val array = data.split("::")
     var dataMap = Map[Int,String]()
     if (array(1)=="F"){
     dataMap=Map(array(0).toInt -> array(1))
    }
    return dataMap
    }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Creation of datanmaps that map data taking in arguments(mapped as key value pairs) 
   
   
   def Pushkal2ndDataMap(data:String): Map[Int,Int] = {
     val array1 = data.split("::")     
     val dataMap1 = Map[Int,Int](
     (array1(0).toInt->(array1(1).toInt))     
    )
     return dataMap1
    }  
  

   ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Creation of datanmaps that map data taking in arguments(mapped as key value pairs)
   def Pushkal3rdDataMap(d1:Int , d2:Int): Map[Int,Int] = {
     val dataMap3 = Map[Int,Int](
     ((d1->d2))
    )
     return dataMap3
    } 
     
  
  
   def Apriori(userdata : Iterator[(Int,Set[Int])],support:Float ): Map[Set[Int], Int]={
     
      
     var candidates = HashMap[Set[Int], Int]()
     var frequent = new ListBuffer[Set[Int]]()
     var freq1copy = Set[Int]()
     var mainlist = new ListBuffer[Set[Int]]()
     var touselist = new ListBuffer[Set[Int]]()
     for (us <- userdata){
       touselist += us._2
     }
     
     
     
     
     //println(support)
     for (z <- touselist) { //this for
     for (x <- z){  
         var s=candidates.getOrElse(Set(x), 0) + 1
         candidates += Set(x) -> s
         //println(candidates.length)
       }
     } //this for ends
     
     //candidates.foreach {println}
     
      //Collecting singletons with values > threshold
     for ((k,freq) <- candidates){
       if (freq >= support){
        frequent += k
     }}
     
     for (x<- frequent){
     freq1copy = freq1copy.union(x)
     }
     mainlist ++= frequent
    
    
     var count = 1 
     while (!frequent.isEmpty){ //while loop
       var templist = new ListBuffer[Set[Int]]()
       var viset = Set[Set[Int]]()
       candidates = candidates.empty
       for (a <- frequent){
            var xy = freq1copy.diff(a)
            for (item <- xy){  
            var ab = a.union(Set(item))
            if (!viset.contains(ab)){
            
         viset += ab   
         var checkfromfreq = ab.toList.combinations(count).toList
         //println(checkfromfreq,"--------------")
        
        var flag = true
        for (eachelem <- checkfromfreq){//0
         //println(eachelem.toSet,"-----")
         if (!(frequent.contains(eachelem.toSet))){ //1
           flag=false
           
         } //1
        }//0
       
         
        if (flag){
          for (x <- touselist){ 
            if (x.intersect(ab).equals(ab)){
              candidates += ab -> (candidates.getOrElse(ab, 0) + 1)
                               }
                           }
                   }
       
             } 
           }
       }
     
     

 
       frequent = new ListBuffer[Set[Int]]()
       for ((k,freq) <- candidates){
       if (freq >= support){
        frequent += k
       }
       }
       
       
       //if (!frequent.isEmpty){
       mainlist ++= frequent
       count=count+1
          
     }//While ends
     
     var mainlist1=Map[Set[Int],Int]()
     mainlist.foreach(f => mainlist1 += f->1)
     return mainlist1
   }
     
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

   
}
