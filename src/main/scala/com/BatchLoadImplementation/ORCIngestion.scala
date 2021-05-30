package com.BatchLoadImplementation

import com.factoryPattern.BatchFileIngestion
import org.apache.spark.sql.SparkSession

class ORCIngestion(filePath : String, outputPath: String) extends BatchFileIngestion {
  override def dataIngestion: Unit = {
    // Spark Session
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("Reading File in ORC Format").getOrCreate()

    val sourceDF = spark.read
      .orc(filePath)

    sourceDF.show(false)
  }
}
