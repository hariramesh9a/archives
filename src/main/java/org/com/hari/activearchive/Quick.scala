package org.com.hari.activearchive
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object Quick {
  
  def main(args: Array[String]): Unit = {
    
     val dataFile = args(0)
     val conf = new SparkConf().setAppName("Active Archive")
     val sc = new SparkContext(conf)
     val sqCont=new HiveContext(sc)
     sqCont.sql("use ramesh2")
     val df=sqCont.read.format("com.databricks.spark.csv").option("header", "true").load(dataFile)
     df.write.mode("overwrite").saveAsTable("testspark")
     
     
    
  }
  
}