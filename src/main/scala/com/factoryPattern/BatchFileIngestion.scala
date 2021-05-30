package com.factoryPattern

import com.BatchLoadImplementation.{JsonIngestion, XMLIngestion}
import com.configuration.GenericConfiguration

abstract class BatchFileIngestion {
  def dataIngestion
}

object BatchFileIngestion {

  def apply(fileType : String, filePath : String, outputPath: String)= {

    val constantObject = ConstantsImmut(filePath, outputPath, Some(fileType), None)
    constantObject match {
      case ConstantsImmut(filePath, outputPath, Some(GenericConfiguration.FILE_TYPE_JSON), None) =>new JsonIngestion(filePath : String, outputPath : String).dataIngestion
      case ConstantsImmut(filePath, outputPath, Some(GenericConfiguration.FILE_TYPE_XML), None) =>new XMLIngestion(filePath : String, outputPath : String).dataIngestion
      case _            => println("There is no match found based on the file Type")
    }
  }
}