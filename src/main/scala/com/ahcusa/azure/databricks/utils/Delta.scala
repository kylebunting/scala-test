package com.ahcusa.azure.databricks.utils

import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession

object Delta {
  def saveAsDeltaTable(tableName: String, sourcePath: String, targetPath: String, mode: String = "overwrite", sourceFormat: String = "avro"): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    // Create a DataFrame containing the data from the source location
    val dfDelta = spark.read.avro(s"$sourcePath/$tableName/*.$sourceFormat")
    val targetDeltaLocation = s"$targetPath/$tableName"

    // If overwriting, remove the existing Delta table and underlying files/directory
    if (mode == "overwrite") {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      Dbfs.rm(targetDeltaLocation)
    }

    // Write the DataFrame to the target location in Delta format
    dfDelta.write
      .format("delta")
      .mode(mode)
      .save(targetDeltaLocation)

    if (mode == "overwrite") {
      // Create a Delta table for the specified tableName
      spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$targetDeltaLocation'")
    }
  }
}