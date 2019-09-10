

import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.Console
import java.io._
import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.graphframes.GraphFrame
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable.ListBuffer
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.Map.WithDefault
import scala.math.BigDecimal
import sun.security.provider.certpath.AdjacencyList
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.Row

//Community Pushkal

object Community {
  var edgeweight = scala.collection.mutable.HashMap[(Int,Int),Float]()
  var toprint = scala.collection.mutable.HashMap[(Int,Int),Float]()
  var user_rdd_adjencylist = scala.collection.mutable.HashMap[Int,scala.collection.mutable.ListBuffer[Int]]()
  var sorted_betweenness = scala.collection.mutable.HashMap[Float,scala.collection.mutable.ListBuffer[(Int,Int)]]()
  var Module_map = scala.collection.mutable.HashMap[Float,ListBuffer[ListBuffer[Int]]]()
  def main(args: Array[String]) {
    val start = System.nanoTime
    var mp = scala.collection.immutable.Map[(Int,Int),Int]()
    
    
    //val txtFile = args(1)
    val txtFile = args(0)
    val betweenness_file = args(2)+"Pushkal_Bhatia_betweenness.txt"
    val community_file = args(1)+"Pushkal_Bhatia_communities.txt"
    val conf = new SparkConf().setAppName("Pushkal 2nd Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //var filetouse = sc.textFile(txtFile,4).cache()   //filetouse
    
    //val original =  filetouse.flatMap(x => Pushkal1stDataMap(x)).collectAsMap()
    
    var txtFile1 = sc.textFile(txtFile,5).cache()
    
    var movie_head = txtFile1.first()

    var file_without_head = txtFile1.filter(row => row != movie_head) //
    
    //file_without_head.take(20).foreach(println)
    
    // Our Original RDD [Userid,MoveId,Ratings] file without head RDD
    var file_without_head_rd = file_without_head.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt, x.split(",")(2).toDouble)) //originalRDD
    
    
    //User_with_ratedmovies-List
    var user_movie = file_without_head_rd.map(x => (x._1, x._2)).groupByKey()
    
    //User_with_ratedMovies_asSet
    
    var user_movie_as_set = user_movie.map(x => (x._1, x._2.to[collection.mutable.Set]))
    //user_movie_as_set.take(20).foreach(println)
    
    // Collecting as map
    var user_movie_as_set_map = user_movie.collectAsMap()
    
    //Number of distint users:
    var unique_users = file_without_head_rd.map(x => x._1).distinct()
    
    //creating pairs of each user with another
    var distinct_user_pairs = unique_users.toLocalIterator.toSet.subsets(2)
    //distinct_user_pairs.foreach(println)
    
    
    //Map to store, user and other user, if their number of intersected movies > 3
    var user_user_with_intercount = scala.collection.mutable.Map[(Int, Int), Int]()
    
    //List of users and edges
    var nodes = scala.collection.mutable.Set[(Int)]()
    var edges = scala.collection.mutable.Set[(Int,Int)]()
    
    for (x <- distinct_user_pairs ){
      var u1 = x.toList(0)
      var u2 = x.toList(1)

      var intersection_count = user_movie_as_set_map.get(u1).get.toSet.intersect(user_movie_as_set_map.get(u2).get.toSet).size
    
      if (intersection_count > 2){
        user_user_with_intercount.put((u1, u2), intersection_count)
        nodes += (u1)
        nodes += (u2)
        edges += ((u1,u2))
        edges += ((u2,u1))
        user_user_with_intercount.put((u2, u1), intersection_count)

      }
    }
    



    for (i <- user_user_with_intercount ){
        if (user_rdd_adjencylist.contains(i._1._1))
          user_rdd_adjencylist(i._1._1.toInt) += i._1._2.toInt
        else
          user_rdd_adjencylist(i._1._1.toInt) = ListBuffer((i._1._2.toInt))
       }
    
    
      /* External graph =------------------------------------------------------------------------------------------------ 
      user_rdd_adjencylist = scala.collection.mutable.HashMap[Int,scala.collection.mutable.ListBuffer[Int]]()
      nodes = scala.collection.mutable.Set[Int]()
      edges = scala.collection.mutable.Set[(Int,Int)]()
      user_rdd_adjencylist += 1 -> ListBuffer[Int](2,3)
      user_rdd_adjencylist += 2 -> ListBuffer[Int](1,3)
      user_rdd_adjencylist += 3 -> ListBuffer[Int](1,2,7)
      user_rdd_adjencylist += 4 -> ListBuffer[Int](5,6)
      user_rdd_adjencylist += 5 -> ListBuffer[Int](4,6)
      user_rdd_adjencylist += 6 -> ListBuffer[Int](4,5,7)
      user_rdd_adjencylist += 7 -> ListBuffer[Int](3,6,8)
      user_rdd_adjencylist += 8 -> ListBuffer[Int](7,9,12)
      user_rdd_adjencylist += 9 -> ListBuffer[Int](8,10,11)
      user_rdd_adjencylist += 10 -> ListBuffer[Int](9,11)
      user_rdd_adjencylist += 11 -> ListBuffer[Int](9,10)
      user_rdd_adjencylist += 12 -> ListBuffer[Int](8,13,14)
      user_rdd_adjencylist += 13 -> ListBuffer[Int](12,14)
      user_rdd_adjencylist += 14 -> ListBuffer[Int](12,13)
      nodes = scala.collection.mutable.Set[Int](1,2,3,4,5,6,7,8,9,10,11,12,13,14)
      edges = scala.collection.mutable.Set[(Int,Int)]((1,2),(2,1),(1,3),(3,1),(2,3),(3,2),(3,7),(7,3),(6,7),(7,6),(4,6),(6,4),(4,5),(5,4),(5,6),(6,5),(7,8),(8,7),(8,9),(9,8),(8,12),(12,8),(9,10),(10,9),(9,11),(11,9),(10,11),(11,10),(12,13),(13,12),(12,14),(14,12),(13,14),(14,13))
      println("edges.size: "+edges.size)
      
     user_rdd_adjencylist = scala.collection.mutable.HashMap[Int,scala.collection.mutable.ListBuffer[Int]]()
      nodes = scala.collection.mutable.Set[Int]()
    user_rdd_adjencylist += 1 -> ListBuffer[Int](2,3)
	  user_rdd_adjencylist += 2 -> ListBuffer[Int](1)
	  user_rdd_adjencylist += 3 -> ListBuffer[Int](1,4)
	  user_rdd_adjencylist += 4 -> ListBuffer[Int](3)
	   nodes = scala.collection.mutable.Set[Int](1,2,3,4)
	  edges = scala.collection.mutable.Set[(Int,Int)]((1,2),(2,1),(1,3),(3,1),(3,4),(4,3))
      //Exteranal Graph -=----------------------
       //---------------------------------------------------------------------------
    //Running BFS:*/
      
      
    //var nodelist = sc.parallelize(nodes.toList,4).toDF("id").cache()
    //var edgelist = sc.parallelize(edges.toList,4).toDF("src","dst").cache()
    //edgelist.drop("_id")
    //var g = GraphFrame(nodelist, edgelist)  
    
    
    def BFS(user_rdd_adjencylistx: scala.collection.Map[Int,Iterable[Int]],x : Int) = {
      
      var visited = scala.collection.mutable.HashMap[Int,Int]()
      var level = scala.collection.mutable.HashMap[Int,Int]()
      var score = scala.collection.mutable.HashMap[Int,Float]()
      var scoreforbtw = scala.collection.mutable.HashMap[Int,Float]()
      var listofpopped = new scala.collection.mutable.ListBuffer[Int]()
      var sta : Stack[Int] = Stack()
      var l = 0
      
      for (z <-user_rdd_adjencylistx ){
        visited += z._1 -> 0
        level += z._1 -> 0
        score += z._1 -> 0
        scoreforbtw += z._1 -> 1
      }
      
      //visited.foreach(println)
      
      var que : Queue[Int] = Queue()
      
      que.enqueue(x)
      level(x) = 0
      score(x) = 1
      
      visited(x) = 1
      
      //println(que,"que")
      while(que.nonEmpty){
        var s = que.dequeue()
        sta.push(s)
//        println("guch")
        for (i <- user_rdd_adjencylistx(s)){
          if (visited(i) == 0)
          {
          level(i) = level(s) + 1
          que.enqueue(i)
          visited(i) = 1
          
          for (z <- user_rdd_adjencylistx(i)){
           if (level(i) > level(z))
             score(i) += score(z)
          }
       }
        }
        
        
      }

     
     
     //println(score)
     
     while(sta.nonEmpty){
       var s = sta.pop()
       for (x <-user_rdd_adjencylistx(s) ){
         if (level(x) == level(s)-1){
           scoreforbtw(x) += ((score(x)/score(s)) * scoreforbtw(s))
           if (!edgeweight.contains((x,s))){
             edgeweight((x,s)) = (score(x)/score(s) * scoreforbtw(s))
             edgeweight((s,x)) = (score(x)/score(s) * scoreforbtw(s))
           }
           else
           {
             edgeweight((x,s)) += (score(x)/score(s) * scoreforbtw(s))
             edgeweight((s,x)) += (score(x)/score(s) * scoreforbtw(s))
           }
         }
       }
       
     }
     

      } 
    
    
   
    def betweenness(user_rdd_adjencylist1: scala.collection.Map[Int,Iterable[Int]])= {
    for (c <- user_rdd_adjencylist1)
    BFS(user_rdd_adjencylist1,c._1)
    
     for (q <- edgeweight){
     var z = BigDecimal(q._2/2).setScale(1,BigDecimal.RoundingMode.FLOOR).toFloat
     if (sorted_betweenness.contains(z))
       sorted_betweenness(z) += ((q._1._1,q._1._2))
     else
       sorted_betweenness(z) = ListBuffer((q._1._1,q._1._2))
     }
    }
    
    betweenness(user_rdd_adjencylist)
    //edgeweight.foreach(println)
    //sorted_betweenness.foreach(println)
    
   
    for (x <- edgeweight){
      if ((x._1._1) < (x._1._2))
        if (!toprint.contains(x._1))
      toprint(x._1) = BigDecimal((edgeweight(x._1)/2)).setScale(1,BigDecimal.RoundingMode.FLOOR).toFloat
      }
    
    val pw1 = new PrintWriter(new File(betweenness_file))
    toprint.toSeq.sortBy(f=>f._1).foreach(f => pw1.write("("+ f._1._1 + "," + f._1._2+ "," + f._2 +")" +"\n"))
    pw1.close()
    

  def find_modularity(adj_list:scala.collection.mutable.ListBuffer[Int], edgeli:scala.collection.mutable.Set[(Int,Int)]) : Double={
  var Q = 0.toDouble
  var A = 0
  for (ii <- adj_list){
    for (jj <- adj_list){
      //if (ii != jj){
        if ((Set((ii,jj)).intersect(edgeli)).size >0){
          A = 1
        }
        else{
          A = 0
        }
        Q += (A - (((user_rdd_adjencylist(ii).size)* (user_rdd_adjencylist(jj).size )).toDouble/(edgeli.size)))
        //println( (A - (((adj_list(ii).size)* (adj_list(jj).size )).toFloat/(2*edge.size))))
          //}
    }
  }
  return(Q)
  }
    
   
    def BFS_for_community(user_rdd_adjencylist2: scala.collection.Map[Int,Iterable[Int]],x : Int) : scala.collection.mutable.Set[Int] = {
      

      var visited1 = scala.collection.mutable.HashMap[Int,Int]() 
      var sta = scala.collection.mutable.Set[Int]()
      for (z <-user_rdd_adjencylist2 )
        visited1 += z._1 -> 0

        var que1 : Queue[Int] = Queue()
      
        que1.enqueue(x)
        visited1(x) = 1
        
        while(que1.nonEmpty){
        var s = que1.dequeue()
        sta += s
//        println("guch")
        for (i <- user_rdd_adjencylist2(s)){
          if (visited1(i) == 0)
          {
          que1.enqueue(i)
          visited1(i) = 1

       }
        }
        
        
      }
        
        return sta
        
    } 
  
    var community_map = scala.collection.mutable.HashMap[Double, scala.collection.mutable.HashMap[Int,scala.collection.mutable.Set[Int]]]()
    var best_community = scala.collection.mutable.HashMap[Int, scala.collection.mutable.Set[Int]]()
    var Q_best = -999.99;
    var flag = true
    
    //sorted_betweenness.nonEmpty
    while (sorted_betweenness.nonEmpty && flag){
      
      var color =0
      //println("sorted btweenness")
      //sorted_betweenness.foreach(println)
      var mx = sorted_betweenness.maxBy(f=> f._1)
      //println(mx)
      var xlist = scala.collection.mutable.ListBuffer[(Int,Int)]()
      xlist = mx._2
      //println(xlist)

      for (a <- xlist){
        //println("Edges remove: "+a)
        user_rdd_adjencylist(a._1) -= (a._2)        //remove edges from edgelist and adjency list
        //(a._2) -= (a._1)
        edges -= ((a._1,a._2))
        //edges -= ((a._2,a._1))

      }

      
      sorted_betweenness.remove(mx._1)
      var result2 = scala.collection.mutable.HashMap[Int,scala.collection.mutable.Set[Int]]()
      var nodesforwhile = user_rdd_adjencylist.keys.toSet
      //println("nodes: " + nodes)
      while (nodesforwhile.nonEmpty){                    //Doing BFS till all nodes are covered
        
         var covered = scala.collection.mutable.Set[(Int)]()
         
         covered = BFS_for_community(user_rdd_adjencylist,nodesforwhile.head)
         //println("covered:  ",covered)
         result2(color) = covered
         color += 1
         
         for (c <- covered){
         nodesforwhile -= c
         }
         
         
         }
      
      
      //println("result2" + result2.toString)
      //------------------------------calculate modularity--------------------------------------
      
      
      var Q_use = 0.0.toDouble
      
      for (com <- result2)
      Q_use += find_modularity(com._2.to[scala.collection.mutable.ListBuffer], edges)
      
      Q_use = Q_use /(edges.size)
      
      //println("Modularity : " + Q_use)
      //println("Communities for this modularity")
      //println(result2)
      
      //community_map += ((Q_use, result2))}
      if(Q_use > Q_best)
      {
        best_community = result2
        Q_best = Q_use

      }
      
       //println("Q is : "+Q+" Q_best is "+Q_best)


      if(Q_use<Q_best)
        flag = false  
      
    }//While ends
    
    
    val pw = new PrintWriter(new File(community_file))
    
    //var best_community1 =  community_map.maxBy(f=> f._1)
    //println(best_community1._1) 
    //best_community = best_community1._2
    var finalcommunity = best_community.map(x => x._2.to[scala.collection.mutable.ListBuffer].sorted)

    var finalcommunity1 = finalcommunity.toSeq.sortBy(f=>f(0))
    for(i <- finalcommunity1)
      {pw.write(i.mkString("[",",","]"))
      pw.write('\n')}
      pw.close()
    
    
  //val end = (System.nanoTime -start) / 1e9d
  //println("The total execution time taken is " + end + " sec")  
    
    
}//End of main
}
  
