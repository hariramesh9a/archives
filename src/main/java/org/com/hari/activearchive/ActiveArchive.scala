package org.com.hari.activearchive
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.File
import org.apache.spark.sql.functions._

object ActiveArchive {

  def main(args: Array[String]): Unit = {

    val dataFile = args(0)
    val modelFile = args(1)
    val projectName = args(2)
    val tableName = args(3)
    val partName = args(4)
    val loadEvent: String = args(5)
    val tblPath = "/user/hive/" + projectName + "/" + tableName + "/" 
    val opPath = tblPath + partName + "/" + loadEvent+"/"
    val conf = new SparkConf().setAppName("Active Archive").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlCont = new HiveContext(sc)
    val delimiter = getDelimiter(modelFile)
    val headerOption = getHeaderOption(modelFile)
    val schema = getSchema(modelFile)
    val myData = sqlCont.read.format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", headerOption)
      .option("treatEmptyValuesAsNulls", "true")
      .option("ignoreTrailingWhiteSpace", "true")
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

    //    
    //    sqlCont.setConf("hive.exec.dynamic.partition", "true")
    //    sqlCont.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val dbString = "CREATE  DATABASE IF NOT EXISTS " + projectName
    sqlCont.sql(dbString)
    sqlCont.sql("use " + projectName)
    val tblString = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + formattedColString + ")PARTITIONED BY(batchdate STRING, loadeventid STRING) STORED AS PARQUET LOCATION '"+tblPath+"'" 
    sqlCont.sql(tblString)
    myData.write.mode("overwrite").parquet(opPath)
    val partString = "ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "') location '" + opPath + "'"
   sqlCont.sql(partString)    
    val addPartString="LOAD DATA LOCAL INPATH '"+opPath+"' OVERWRITE INTO TABLE "+tableName+" PARTITION (batchdate='" + partName + "', loadeventid='" + loadEvent + "')"
    sqlCont.sql(addPartString) 
    val hiveQL = sqlCont.read.table(tableName)
    hiveQL.show()
    println(hiveQL.count())
    hiveQL.printSchema()

  }

  def getDelimiter(modelFileName: String): String = {
    var delimiter = ","
    if (new java.io.File(modelFileName).exists) {
      for (line <- Source.fromFile(modelFileName).getLines().take(1)) {
        var fileArray = line.split(":")
        if (fileArray.size.>=(1) && fileArray(0).toLowerCase().trim() == "delimiter") {
          delimiter = fileArray(1)
        }
      }
    }
    return delimiter
  }

  def getHeaderOption(modelFileName: String): String = {
    var header = "false"
    if (new java.io.File(modelFileName).exists) {
      var i = 0;
      for (line <- Source.fromFile(modelFileName).getLines().take(2)) {
        i += 1
        if (i == 2) {
          var fileArray = line.split(":")
          if (fileArray.size.>=(1) && fileArray(0).toLowerCase().trim() == "header") {
            header = fileArray(1)
          }
        }
      }
    }
    return header
  }

  def getSchema(modelFileName: String): StructType = {
    var schema = StructType(Array(StructField("Column00", StringType, false)))
    if (new java.io.File(modelFileName).exists) {
      var i = 0;
      for (line <- Source.fromFile(modelFileName).getLines().take(3)) {
        i += 1
        if (i == 3) {
          val schemaLine = line //+ ":batchdate:loadeventid"
          schema = StructType(
            schemaLine.split(":").map(fieldName => StructField(fieldName.toLowerCase(), StringType, false)))
        }
      }
    }
    return schema
  }

}