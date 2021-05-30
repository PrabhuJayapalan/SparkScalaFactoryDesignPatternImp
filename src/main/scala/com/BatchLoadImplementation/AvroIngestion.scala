package com.BatchLoadImplementation

import com.factoryPattern.BatchFileIngestion
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._

class AvroIngestion(filePath : String, outputPath: String) extends BatchFileIngestion {
  override def dataIngestion: Unit = {
    // Spark Session
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("Reading File in Avro Format").getOrCreate()

    val sourceDF = spark.read
      .avro(filePath)

    sourceDF.show(false)
  }
}
