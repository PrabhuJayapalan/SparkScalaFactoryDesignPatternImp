package com.factoryPattern

case class ConstantsImmut(filePath: String,
                          outputPath: String,
                          fileType: Option[String] = None,
                          loadType: Option[String] = None)