package org.com.hari.activearchive
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
object CheckHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Check Hive").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlCont = new HiveContext(sc)
    val sqldb=sqlCont.sql("show databases")
    sqldb.foreach { println }
  }
}