package com.driverPipeLine

import com.configuration.GenericConfiguration
import com.factoryPattern.{BatchFileIngestion, ConstantsImmut}

object DriverEngine {
  def main(args: Array[String]): Unit = {

    if(args.length < 4){
      println("Arguments are not Sufficient. Required 'load_Type, File_Type, File_Path, Output_Path'")
      println("################  To make the File load successful  ######################")
      System.exit(1)
    }

    val loadType = args(0)
    val fileType = args(1)
    val filePath = args(2)
    val outputPath = args(3)

    val constantObject = ConstantsImmut(filePath, outputPath, Some(fileType), Some(loadType))

    constantObject match {
      case ConstantsImmut(filePath, outputPath, Some(fileType), Some(GenericConfiguration.LOAD_TYPE_BATCH)) => BatchFileIngestion(fileType, filePath, outputPath)
    }
  }
}
