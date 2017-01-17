package dftry
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by shikhar on 17/1/17.
  */
object DataFrameTry {
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DataFrameTry")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personDemographicsCSVPath = "/home/shikhar/Downloads/person-demo.csv"
    val readDF = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header","true")
                .load(personDemographicsCSVPath)
    //readDF.show()
    val personHealthCSVPath = "/home/shikhar/Downloads/person-health.csv"
    val readDF2= sqlContext.read.format("com.databricks.spark.csv").option("header","true").load(personHealthCSVPath)
    //readDF2.show()
    //Method 1 #join
    //val personDF = readDF.join(readDF2, readDF("id")=== readDF2("id"), "left_outer")
    //Method 2 _use this as the healthrecords would be much much larger than the demographics data (many to one mapping and hence use the second method
    val personDF2 = readDF2.join(readDF, readDF("id") === readDF2("id"),"right_outer").drop(readDF2("id"))
    //val ageLessThan50DF = personDF2.filter(personDF2("age")<50)
    //val personDF3 = readDF2.join(readDF, readDF("id")=== readDF2("id"),"inner") //ONLY CONSIDERS THE ROWS WHICH HAVE ID'S DEFINED IN BOTH THE TABLES
    //ageLessThan50DF.show()

    //personDF.show()
    //personDF2.show()
    //personDF3.show()
    //ageLessThan50DF
    //  .coalesce(1)
    //  .write
    //  .format("com.databricks.spark.csv")
     // .option("header","true")
      //.save("/home/shikhar/opCSV")
//    ageLessThan50DF
//      .coalesce(10)
//      .write
//      .format("com.databricks.spark.csv")
//      .option("header","true")
//      .save("/home/shikhar/opCSV1")
//    ageLessThan50DF
//      .write
//      .mode(SaveMode.Overwrite) //to overwrite the csv if saved to a given directory
//      .format("com.databricks.spark.csv")
//      .option("header","true")
//      .save("/home/shikhar/opCSV2")
//    //Rading person-insurance.csv
    val personInsuranceCSVPath = "/home/shikhar/Downloads/person-insurance.csv"
    val readDF3= sqlContext.read.format("com.databricks.spark.csv").option("header","true").load(personInsuranceCSVPath)
    //Joining person-insurance.csv to the other two csv
    val personDF3 = personDF2.join(readDF3, personDF2("id") === readDF3("id"),"right_outer").drop(personDF2("id"))
    //validating the date
    val readDf3_valid = readDF3.filter(personDF3("datevalidation")>= "2017-01-11").drop("id").drop("datevalidation")
    //calculating the amount insured by each payer
    readDf3_valid.show()
    val rdd = readDf3_valid.map(t => (t(0).toString,t(1).toString.toInt))
    val fin = rdd.reduceByKey(_+_)
    //Priting to CSV
    //val struct = new StructType(Array(StructField("payer",StringType,nullable = true),StructField("Amount",LongType,nullable = true)))
    //val newDF = sqlContext.createDataFrame(fin,struct)
    val m = fin.map(a => a._1 + "," + a._2).coalesce(1)
    m.saveAsTextFile("Sum")
  }
}