package com.ahcusa.azure.databricks.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object Secrets {
  def getSecret(key: String, scope: String = "key-vault-secrets"): String = {
    dbutils.secrets.get(scope, key)
  }
}
