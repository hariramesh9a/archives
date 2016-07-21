package org.com.hari.activearchive
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object TestDir {
 
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("TEst").setMaster("local[*]")
     val sc = new SparkContext(conf)
    val file="src/main/resources/sample.dat"
    val myRDD=sc.textFile(file)
    val myData=myRDD.map { x => x.split(",") }
myData.foreach { x => if(x.size.!=(2)){println(x(0))}  }
    
    
  }
  
}