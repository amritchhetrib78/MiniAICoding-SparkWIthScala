package miniaicoding.wk1.sparkstreaming
/*
Author : Amrit Chhetri, AI Forensic Researcher | License: MTI, GNU, OSL
Purpose: CSV/Text Files Streaming using Spark Streaming to CDW | Code Plagiarism: 3.0
 */
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object SparkStreamingCSV {
  def main(args:Array[String]):Unit={
    //Session from SparkContext
    /*
    val sparkCnf = new SparkConf()
      .setAppName("Spark Streaming")
      .setMaster("local[*]")
    val sparkCtx = new SparkContext(sparkCnf)
    val sparkSxn = SparkSession.builder().getOrCreate()
    sparkCtx.setLogLevel("ERROR")
         */

    //Session with master

    val sparkSxn: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Streaming")
      .getOrCreate()
    sparkSxn.sparkContext.setLogLevel("ERROR")


    // Schema definition
    val schemaObj = StructType(Array(
      StructField("OS",StringType,true),
      StructField("Version",StringType,true),
      StructField("Cost",IntegerType,true)
    ))

    val df = sparkSxn
      .readStream
      .format("csv")
      .schema(schemaObj)
      .load("file:///C:/MiniAICoding-WAC/data/input")

    df.writeStream.format("console")
      .option("checkpointLocation",
        "file:///C:/MiniAICoding-WAC/data/output")
      .start()
      .awaitTermination()
  }
}
