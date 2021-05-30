package com.Utility

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions.{col,explode,explode_outer,to_json}

class NestedFlattening {

  /* Flattening the Nested Files through Spark SQL*/
  def flatteningThrSparkSQL(sourceDF : DataFrame): DataFrame ={
    var processedDF = sourceDF

    var flag = true //Flag to determine the closure of iteration
    while(flag){
      flag = false
      processedDF.schema.fields.foreach {
      elements =>
        var fieldNames =processedDF.schema.fields.map(x => x.name)

        elements.dataType match {
        case arrayType: ArrayType => // Flatten the Array
          flag = true
          fieldNames = fieldNames.filter(_!=elements.name) ++ Array("explode_outer(".concat(elements.name).concat(") as ").concat(elements.name))
          processedDF = processedDF.selectExpr(fieldNames:_*)
        case structType: StructType => // Flatten the Struct
          flag = true
          processedDF.show()
          fieldNames =fieldNames.filter(_!=elements.name) ++ structType.fieldNames.map(childNames =>
                      elements.name.concat(".").concat(childNames)
                      .concat(" as ").concat(elements.name).concat("_").concat(childNames))

          processedDF = processedDF.selectExpr(fieldNames:_*)
        case _=> // dataTypes are unexpected
      }
    }
    }
    processedDF
  }

  /* Flattening The Nested Files through Spark DataFrame */
  def flatteningThrSparkDataFrame(sourceDF : DataFrame): DataFrame={
    var processedDF = sourceDF
    var schema: StructType =processedDF.schema

    var flag = true //Flag to determine the closure of iteration
    while(flag){
      flag = false //reset every loop
      schema.fields.foreach {
        elements =>
          elements.dataType match {
            case arrayType: ArrayType =>
              flag = true
              processedDF = processedDF.withColumn(elements.name+"_temp", explode_outer(col(elements.name)))
                .drop(col(elements.name)).withColumnRenamed(elements.name+"_temp", elements.name)
            case structType: StructType =>
              flag = true
              structType.fields.foreach {
              innerElement =>
                processedDF = processedDF.withColumn(elements.name+"_"+innerElement.name, col(elements.name+"."+innerElement.name))
              }
              processedDF = processedDF.drop(col(elements.name))
            case _=> // dataTypes are unexpected
          }
          }
      schema = processedDF.schema
      }
    processedDF
  }

  /*Flattening through iterating through for loop*/
  def flatteningThrTailRecursive(sourceDF : DataFrame):DataFrame={
    var processedDF = sourceDF
    val fields = processedDF.schema.fields
    val fieldNames =fields.map( x => x.name)

    //iteration for flattening
    for(i <- 0 to fieldNames.length-1){
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name

      fieldType match {
        case arrayType: ArrayType =>
          val newFieldNames = fieldNames.filter(_!=fieldName) ++ Array("explode_outer(".concat(fieldName).concat(") as ").concat(fieldName))
          val explodedDF = processedDF.selectExpr(newFieldNames:_*)
          return flatteningThrTailRecursive(explodedDF)
        case structType: StructType => //println("flatten struct")
          val newFieldNames = fieldNames.filter(_!= fieldName) ++
            structType.fieldNames.map(childName => fieldName.concat(".").concat(childName)
              .concat(" as ")
              .concat(fieldName).concat("_").concat(childName))
          val explodeDF = processedDF.selectExpr(newFieldNames:_*)
          return flatteningThrTailRecursive(explodeDF)
        case _ => //println("other type")
      }
    }
    processedDF
  }
}
