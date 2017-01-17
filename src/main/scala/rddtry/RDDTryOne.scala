package rddtry

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shikhar on 17/1/17.
  */
object RDDTryOne {
  def  main(args: Array[String]): Unit = {

    //val resourcePath = getClass.getResource("").getPath()
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc = new SparkContext(conf)

    //Reading Text File
    val ipPath = "/home/shikhar/warandpeace.txt"
    val sampleRdd = sc.textFile(ipPath)
      .cache() //helps in preventing the redundant writing of the data on every operation
    //val sampleRdd = readRdd.sample(withReplacement =  false, 0.01,123)
    //Initial 10 lines
    //val xRdd = readRdd.take(10)
    //xRdd.foreach(println)

    //taking samples
    //val yRdd  = readRdd.sample(withReplacement = false, 0.1,123) //lazy, Transformation
    //val zRdd = readRdd.takeSample(withReplacement = false, 10,123)//Action, seed parameter to ensure that the output is constant, Active evaluaiton
    //yRdd.foreach(println)

    //Counting Lines
    //println("total count " + readRdd.count())
    //println("sample count "  + sampleRdd.count())
    // Filter Stop Words
    //1. Split Text in Words
    //Method 1
    val splitRdd  = sampleRdd.flatMap(x=>x.toLowerCase().split(" ")).cache()
    //splitRdd.take(10).foreach(println)
    // Method 2
    //val splitRdd = sampleRdd.map(x=>x.split(" ").toList)
    //val opRdd = splitRdd.flatMap(x=>x)
    println("total count"+splitRdd.count())
    val stop_words = List("a", "an", "the", "is","are")
    val filterRDD = splitRdd.filter(x=> !stop_words.contains(x))
    println("Filtered Word Count "+ filterRDD.count())


    // Word Count
    val word = filterRDD.map(x=>(x,1))
    val wrdfrqRdd = word.reduceByKey(_+_).cache()
//    val wrdfrqRdd = word
//                .groupBy(X=>X._1)
//                .map(x=> {
//                  val key = x._1
//                  val totalCount = x._2.size
//                  (key,totalCount)
//                }
//                )
    //wrdfrqRdd.foreach(println)
    val wordfrqRdd = wrdfrqRdd.map(_.swap).map(x=>(x._1,1))
    wordfrqRdd.saveAsTextFile("Word_Freq_Distribution")
//    val frqRdd = wordfrqRdd
//                    .groupBy(x=>x._1)
//                    .map(x=>{
//                      val key = x._1
//                      val freq = x._2.size
//                      (key,freq)
//                    })
////    //frqRdd.saveAsTextFile("/home/txt/abc.txt")
    val frqRdd = wordfrqRdd.reduceByKey(_+_).sortBy(x=>x._1,true,numPartitions = 1)
    frqRdd.foreach(println)
    frqRdd.saveAsTextFile("frequency_distribution")
    val m = wrdfrqRdd.count()
    val x = m/2
    val percentile = wrdfrqRdd.sortBy(x=>x._2, ascending = false)
    val k = sc.parallelize(percentile.take(x.toInt).toSeq).coalesce(1)
    k.saveAsTextFile("Top_50_Percentile")
    //percentile.foreach(println)
  }



}
