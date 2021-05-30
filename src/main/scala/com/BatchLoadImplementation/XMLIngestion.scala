package com.BatchLoadImplementation

import com.Utility.NestedFlattening
import com.factoryPattern.BatchFileIngestion
import org.apache.spark.sql.{SaveMode, SparkSession}

class XMLIngestion(filePath : String, outputPath : String) extends BatchFileIngestion {
  override def dataIngestion: Unit = {
    //Object for Nested Flattening
    val nestedFlattening = new NestedFlattening

    // Spark Session
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("Batch Process_XML File Ingestion").getOrCreate()

    val sourceDF = spark.read.format("com.databricks.spark.xml")
                  .option("rowTag", "catalog").load(filePath).repartition(4)

        val processedDF = nestedFlattening.flatteningThrSparkSQL(sourceDF)
    //    val processedDF = nestedFlattening.flatteningThrSparkDataFrame(sourceDF)
//    val processedDF = nestedFlattening.flatteningThrTailRecursive(sourceDF)

    // Displaying the Output
    processedDF.show(false)
    processedDF.write.mode(SaveMode.Overwrite).save(outputPath)

  }
}
