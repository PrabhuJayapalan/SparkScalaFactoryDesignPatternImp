package com.hadoopFileGeneration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source

object OrcFileGeneration {
  def main(args: Array[String]): Unit = {
    // Spark Session
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .enableHiveSupport()
      .appName("ORC Input File Generation").getOrCreate()

    // Generating the Schema
    val filename ="C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkScalaFactoryDesignPatternImp\\SchemaFile\\Avro\\schemaFile.json"
    val schemaSource = Source.fromFile(filename).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    // mapping the custom Schema with the CSV file
    val sourceDF = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter",",")
      .schema(schemaFromJson)
      .load("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkScalaFactoryDesignPatternImp\\SourceFile\\HadoopFileConversion\\CardTransaction.txt")

    sourceDF.show(false)

    // Saving the data into Avro Format
    sourceDF.write.partitionBy("Card_Type_Code").orc("C:\\Users\\PrabhuManohari\\OneDrive\\Desktop\\SparkScalaFactoryDesignPatternImp\\OutputDirectory\\HadoopFileFormat\\Orc")
  }
}
