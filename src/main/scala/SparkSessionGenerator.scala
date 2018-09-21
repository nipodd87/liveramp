package com.ignitionone.idrlr


import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql._
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

/**
  * Creating Spark Session
  */
object SparkSessionGenerator extends App {


  def GenerateSparkSession() = {

    val config = globals.config
    val master = config.getString("spark.app.master")
//    val aws_access_key = DefaultAWSCredentialsProviderChain.getInstance().getCredentials.getAWSAccessKeyId
//    val aws_secret_key = DefaultAWSCredentialsProviderChain.getInstance().getCredentials.getAWSSecretKey

    var sparkSess: SparkSession = null

    sparkSess = SparkSession
      .builder
      .master(master)
      .config(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, " -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy")
      .config("spark.shuffle.manager", "tungsten-sort")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.shuffle.blockTransferService", "nio")
      .config("spark.scheduler.mode", "FIFO")
      .appName(globals.appName)
      .getOrCreate()

    sparkSess.sparkContext.setLogLevel("ERROR")
//    sparkSess.sparkContext.hadoopConfiguration.set("fs.s3.awsAccessKeyId", aws_access_key)
//    sparkSess.sparkContext.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", aws_secret_key)
//    sparkSess.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", aws_access_key)
//    sparkSess.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", aws_secret_key)

    sparkSess
  }
}