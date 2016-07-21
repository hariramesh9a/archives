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
    if (args.size < 6) {
      println("Usage: <dataFile><ModelFile><HadoopRoot><DatabaseName><TableName><PartitionName><loadEvent><Compression>")
      sys.exit(1)
    }
    val dataFile = args(0)
    val modelFile = args(1)
    val hadoopRoot = args(2)
    val dbName = args(3)
    val tableName = args(4)
    val partName = args(5)
    val loadEvent: String = args(6)
    val compress: String = args(7)
    val tblPath = hadoopRoot + "/" + tableName 
    val batchPath = tblPath + "/" + partName 
    val opPath = batchPath + "/" + loadEvent
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
    var colString: String = ""
    schema.foreach { x =>
      colString += x.name + " " + x.dataType.typeName + ","
    }
    val formattedColString = colString.dropRight(1)
    mkdirs(tblPath)
    chmod(tblPath)
    mkdirs(batchPath)
    chmod(batchPath)
    mkdirs(opPath)
    chmod(opPath)
    val dbString = "CREATE  DATABASE IF NOT EXISTS " + dbName
    sqlCont.sql(dbString)
    sqlCont.sql("use " + dbName)
    var tblString = "";
    if (compress == "Y") {
      tblString = "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + "(" + formattedColString + ")PARTITIONED BY(batchdate STRING, loadeventid STRING) STORED AS PARQUET LOCATION '" + tblPath + "'"
      myData.write.mode("overwrite").parquet(opPath)
    } else {
      tblString = "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + "(" + formattedColString + ")PARTITIONED BY(batchdate STRING, loadeventid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + delimiter + "' STORED AS TEXTFILE LOCATION '" + tblPath + "'"
      myData.write.mode("overwrite").format("com.databricks.spark.csv")
        .option("header", headerOption)
        .option("delimiter", delimiter)
        .save(opPath)
    }
    chmod(tblPath)
    chmod(batchPath)
    chmod(opPath)
    sqlCont.sql(tblString)
    val dropString = "ALTER TABLE " + tableName + " DROP IF EXISTS PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "')"
    sqlCont.sql(dropString)
    val partString = "ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "') location '" + opPath + "'"
    sqlCont.sql(partString)
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
    println("Delimiter is: " + delimiter)
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
    println("Header option is: " + header)
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
    println("Schema inferred is: " + schema)
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
      fileSystem.mkdirs(path, FsPermission.valueOf("drwxrwxrwx"))
    }
  }

}