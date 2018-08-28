package com.ahcusa.azure.datafactory

import com.ahcusa.azure.databricks.utils.{Dbfs, Delta}

object Pipeline {
  /**
    * Copies raw EDW data from files in the Bronze zone to Databricks Delta (parquet) tables in the Gold zone.
    * @param args
    */
  def EdwBronzeToGold(args: Array[String]): Unit = {
    val pipelineName = args(0)

    if (args.length < 7) {
      println(s"${pipelineName} requires arguments: adlsAccountName: String,\ntenantId: String,\nclientIdSecret: String,\nclientKeySecret: String,\nsourcePath: String,\ntargetPath: String")
      return
    }

    val accountName = args(1)
    val tenantId = args(2)
    val clientIdSecret = args(3)
    val clientKeySecret = args(4)
    val sourcePath = args(5)
    val targetPath = args(6)

    // Ensure the ADLS folders required for the copy operation are mounted in DBFS.
    val sourceMountPoint = Dbfs.mountDataLakeStore(accountName, sourcePath, tenantId, clientIdSecret, clientKeySecret)
    val targetMountPoint = Dbfs.mountDataLakeStore(accountName, targetPath, tenantId, clientIdSecret, clientKeySecret)

    val tableList = Dbfs.getFolderList(sourceMountPoint)

    tableList.par.foreach {
      t => Delta.saveAsDeltaTable(t, sourceMountPoint, targetMountPoint)
    }
  }
}