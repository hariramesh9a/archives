package org.com.hari.activearchive
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.sql.functions._
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission

object ActiveArchive {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")
  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  private val fileSystem = FileSystem.get(conf)
  def main(args: Array[String]): Unit = {

    val dataFile = args(0)
    val modelFile = args(1)
    val projectName = args(2)
    val tableName = args(3)
    val partName = args(4)
    val loadEvent: String = args(5)
    val tblPath = "/user/ramesh2/" + projectName + "/" + tableName + "/"
    val opPath = tblPath + partName + "/" + loadEvent + "/"
    val conf = new SparkConf().setAppName("Active Archive")
    val sc = new SparkContext(conf)
    val sqlCont = new HiveContext(sc)
    val delimiter = getDelimiter(modelFile, sc)
    val headerOption = getHeaderOption(modelFile, sc)
    val schema = getSchema(modelFile, sc)
    val myData = sqlCont.read.format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", headerOption)
      .option("treatEmptyValuesAsNulls", "true")
      .schema(schema)
      .load(dataFile)
    val addLoadEventID = udf { (LoadEventId: String) => loadEvent }
    val addBatchDate = udf { (BatchDate: String) => partName }
    //val myFormattedData = myData.withColumn("batchdate", addBatchDate(myData("batchdate"))).withColumn("loadeventid", addLoadEventID(myData("loadeventid")))
    var colString: String = ""
    schema.foreach { x =>
      colString += x.name + " " + x.dataType.typeName + ","
    }
    val formattedColString = colString.dropRight(1)
    val dbString = "CREATE  DATABASE IF NOT EXISTS " + projectName
    sqlCont.sql(dbString)
    sqlCont.sql("use " + projectName)
    val tblString = "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + "(" + formattedColString + ")PARTITIONED BY(batchdate STRING, loadeventid STRING) STORED AS PARQUET LOCATION '" + tblPath + "'"
    sqlCont.sql(tblString)
    mkdirs(tblPath)
    chmod(tblPath)
    myData.write.mode("overwrite").parquet(opPath)    
    chmod(opPath)
    val dropString = "ALTER TABLE " + tableName + " DROP IF EXISTS PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "')"
    sqlCont.sql(dropString)
    val partString = "ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "') location '" + opPath + "'"
    sqlCont.sql(partString)
    val hiveQL = sqlCont.read.table(tableName)
    hiveQL.show()
    println(hiveQL.count())
    hiveQL.printSchema()
    sc.stop()

  }

  def getDelimiter(modelFileName: String, sc: SparkContext): String = {
    var delimiter = ","
    var path = new Path(modelFileName)
    if (fileSystem.exists(path)) {
      val delimData = sc.textFile(modelFileName, 1).first()
      val delimiterVar = delimData.split(":")
      if (delimiterVar(0).toLowerCase().trim() == "delimiter") {
        delimiter = delimiterVar(1)
      }
    }
    println("Delimiter is: "+delimiter)
    return delimiter
  }

  def getHeaderOption(modelFileName: String, sc: SparkContext): String = {
    var header = "false"
    var path = new Path(modelFileName)
    if (fileSystem.exists(path)) {
      val delimData = sc.textFile(modelFileName, 1).take(2).last
      val delimiterVar = delimData.split(":")
      if (delimiterVar(0).toLowerCase().trim() == "header") {
        header = delimiterVar(1)
      }
    }
    println("Delimiter is: "+header)
    return header
  }

  def getSchema(modelFileName: String, sc: SparkContext): StructType = {
    println("Info: getting Schema from model file")
    var path = new Path(modelFileName)
    var schema = StructType(Array(StructField("Column00", StringType, false)))
    if (fileSystem.exists(path)) {
      val schemaLine = sc.textFile(modelFileName, 1).take(3).last
      schema = StructType(
            schemaLine.split(":").map(fieldName => StructField(fieldName.toLowerCase(), StringType, false)))
    }
    println(schema)
    return schema
  }

  def chmod(folderPath: String): Unit = {
    var path = new Path(folderPath)
    if (fileSystem.exists(path)) {
      fileSystem.setPermission(path, FsPermission.valueOf("drwxrwxrwx"))
    }
  }
  
  def mkdirs(folderPath: String): Unit = {
   var path = new Path(folderPath)
     if (!fileSystem.exists(path)) {
    fileSystem.mkdirs(path,FsPermission.valueOf("drwxrwxrwx"))
     }
  }

}