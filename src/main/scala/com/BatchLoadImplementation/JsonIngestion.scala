package com.BatchLoadImplementation

import com.Utility.NestedFlattening
import com.configuration.GenericConfiguration
import com.factoryPattern.BatchFileIngestion
import org.apache.spark.sql.{SaveMode, SparkSession}

class JsonIngestion(filePath : String, outputPath: String) extends BatchFileIngestion {
  override def dataIngestion: Unit = {
    //Object for Nested Flattening
    val nestedFlattening = new NestedFlattening

    // Spark Session
    val spark: SparkSession = SparkSession.builder().master("local[*]")
                              .appName("Batch Process_JSON File Ingestion").getOrCreate()
    val sourceDF = spark.read.format("json").option("multiLine","true").load(filePath).repartition(4)

//    val processedDF = nestedFlattening.flatteningThrSparkSQL(sourceDF)
//    val processedDF = nestedFlattening.flatteningThrSparkDataFrame(sourceDF)
    val processedDF = nestedFlattening.flatteningThrTailRecursive(sourceDF)

    // Displaying the Output
    processedDF.show(false)
    processedDF.write.mode(SaveMode.Overwrite).save(outputPath)

  }
}