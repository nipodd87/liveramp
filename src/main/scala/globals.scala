package com.ignitionone.idrlr

import com.ignitionone.idr.utilities.auditstore.dao.{ProcessDetailLogDao, ProcessLogDao}
import com.ignitionone.idr.utilities.auditstore.dao.impl.{ConnectionFactoryImpl, ProcessDetailLogDaoImpl, ProcessLogDaoImpl}
import com.typesafe.config.ConfigFactory

package object globals {

  var config = ConfigFactory.load("application.conf")
  val environment = config.getString("app.environment")
  val appName = config.getString("spark.app.appname")
  val pgHost = config.getString("app.postgresAuditServer")
  val pgDatabase = config.getString("app.postgresAuditDatabase")
  val pgUser = config.getString("app.postgresAuditUsername")
  val pgPassword = config.getString("app.postgresAuditPassword")
  val pgPort = 5432
  val sizeOfEachFile = config.getLong("size.of.file")
  val connectionFactory : ConnectionFactoryImpl = new ConnectionFactoryImpl(pgHost,
    pgPort,
    pgDatabase,
    pgUser,
    pgPassword)
  val processLogDao : ProcessLogDao = new ProcessLogDaoImpl(connectionFactory)
  val processDetailLogDao : ProcessDetailLogDao = new ProcessDetailLogDaoImpl(connectionFactory)
}
