package com.ahcusa.azure.databricks.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import scala.util.matching.Regex

object Dbfs {
  /**
    * Mounts a Data Lake Store to the Databricks File System at the location: /mnt/acccountName/directoryPath
    * @param accountName
    * @param directoryPath
    * @param tenantId
    * @param clientIdSecret
    * @param clientKeySecret
    * @return
    */
  def mountDataLakeStore (accountName: String,
                 directoryPath: String,
                 tenantId: String,
                 clientIdSecret: String,
                 clientKeySecret: String): String = {
    val mountPoint = s"/mnt/$accountName/$directoryPath"

    val clientId = Secrets.getSecret(clientIdSecret)
    val clientSecret = Secrets.getSecret(clientKeySecret)

    val mountAccount: Regex = """adl://(?=\w*[.-])([\w.-]*).azuredatalakestore.net/?(?=\w*[-\/]*)([\w-\/]*)""".r

    val source = s"adl://$accountName.azuredatalakestore.net/$directoryPath"
    val extraConfigs = Map(
      "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
      "dfs.adls.oauth2.client.id" -> clientId,
      "dfs.adls.oauth2.credential" -> clientSecret,
      "dfs.adls.oauth2.refresh.url" -> s"https://login.microsoftonline.com/$tenantId/oauth2/token"
    )

    if (!mountPointExists(mountPoint, accountName, directoryPath, mountAccount)) {
      mountToDbfs(mountPoint, source, extraConfigs)
    }

    mountPoint
  }

  /**
    * Mounts a Blob Storage account to the Databricks File System at the location: /mnt/acccountName/directoryPath
    * @param accountName
    * @param containerName
    * @param directoryPath
    * @param secretScope
    * @param clientKeySecret
    * @param accountIsSas
    * @return
    */
  def mountBlobStorage(accountName: String,
                       containerName: String,
                       directoryPath: String,
                       secretScope: String,
                       clientKeySecret: String,
                       accountIsSas: Boolean = false): String = {
    val mountPoint = s"/mnt/$accountName/$containerName/$directoryPath"
    val clientSecret = Secrets.getSecret(secretScope, clientKeySecret)

    val mountAccount: Regex = """wasbs://(.*)@([^.]+).blob.core.windows.net/(.*)$""".r

    val source = s"wasbs://$containerName@$accountName.blob.core.windows.net/$directoryPath"
    val confKey = if (accountIsSas) s"fs.azure.sas.$containerName.$accountName.blob.core.windows.net" else s"fs.azure.account.key.$accountName.blob.core.windows.net"
    val extraConfigs: Map[String, String] = Map(
      confKey -> clientSecret
    )

    // TODO: Need to work out sending in the container name as well.
    if (!mountPointExists(mountPoint, accountName, directoryPath, mountAccount)) {
      mountToDbfs(mountPoint, source, extraConfigs)
    }

    mountPoint
  }

  /**
    * Unmounts the specified mount point in DBFS
    * @param mountPoint
    */
  def unmount (mountPoint: String): Unit = {
    try {
      dbutils.fs.unmount(mountPoint)
    }
    catch {
      case e: Exception =>
        println(s"ERROR: Unable to unmount $mountPoint: ${e.getMessage}")
    }
  }

  private def mountToDbfs(mountPoint: String, source: String, extraConfigs: Map[String, String]): Unit = {
    try {
      dbutils.fs.mount(
        source = source,
        mountPoint = mountPoint,
        extraConfigs = extraConfigs
      )
    }
    catch {
      case e: Exception =>
        println(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
    }
  }

  private def mountPointExists(mountPoint: String, accountName: String, directoryPath: String, mountAccount: Regex): Boolean = {
    var isMounted: Boolean = false

    dbutils
      .fs
      .mounts.find(_.mountPoint == mountPoint)
      .map { mountInfo =>
        val mountPointAccount = mountAccount
        mountInfo.source match {
          case mountPointAccount(a, d) if a == accountName && d == directoryPath =>
            // MountPoint is already mounted correctly.
            isMounted = true
          case _ =>
            // Mounted, but not from the current account, so need to remount.
            unmount(mountPoint)
            isMounted = false
        }
      }

    isMounted
  }

  /**
    * Retrieves a list of folder names under the specified path
    * @param path
    * @return
    */
  def getFolderList(path: String): List[String] = {
    dbutils.fs.ls(path).map(f => f.name.replace("/", "")).toList
  }

  /**
    * Removes the file or directory at the specified path
    * @param path
    */
  def rm(path: String): Unit = {
    dbutils.fs.rm(path, true)
  }
}
