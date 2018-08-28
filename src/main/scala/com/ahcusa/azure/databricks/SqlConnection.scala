package com.ahcusa.azure.databricks

import java.util.Properties

import com.ahcusa.azure.databricks.utils.Secrets
import org.apache.spark.sql.SparkSession

/**
  * Connects to the specified SQL Server instance (uses Azure Key Vault for retrieving db connection secrets)
  * @param hostname
  * @param databaseName
  * @param secretScope
  * @param usernameSecret
  * @param passwordSecret
  * @param port
  * @param spark
  */
class SqlConnection (hostname: String,
                     databaseName: String,
                     usernameSecret: String,
                     passwordSecret: String,
                     port: Int = 1433,
                     spark: SparkSession = SparkSession.builder().getOrCreate()) extends Serializable {
  private val sqlDriverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  Class.forName(sqlDriverClassName)

  val jdbcUsername: String = Secrets.getSecret(usernameSecret)
  val jdbcPassword: String = Secrets.getSecret(passwordSecret)
  val jdbcUrl = s"jdbc:sqlserver://$hostname:$port;database=$databaseName"

  // Create a Properties() object to hold the parameters.
  private val connectionProperties = new Properties()
  connectionProperties.put("user", s"$jdbcUsername")
  connectionProperties.put("password", s"$jdbcPassword")
  connectionProperties.setProperty("Driver", sqlDriverClassName)

  /**
    * Retrieve a list of user tables from the specified SQL database
    * @param excludes: List of tables to exclude from the list
    * @return: Returns a list of tables in the target database
    */
  def getTableList(excludes: String): List[String] = {
    import spark.implicits._
    val query = s"(SELECT TABLE_NAME FROM $databaseName.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT IN ($excludes)) tables"
    val tables = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)

    tables.sort("TABLE_NAME").map(t => t(0).toString).collect().toList
  }

  def executeQuery(query: String): Unit ={
    // TODO: Implement this. Figure out what the return value should look like (probably a DataFrame)
    throw new NotImplementedError("The ExecuteQuery method has not been implemented.")
  }
}