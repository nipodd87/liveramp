package com.ignitionone.idrlr


import java.sql.Timestamp
import java.util

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.ignitionone.idr.utilities.auditstore.IdrProcessStatus
import com.ignitionone.idr.utilities.auditstore.model.impl.ProcessLogImpl
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer


object SORGenerator {

  val config = globals.config

  private val processLogDao = globals.processLogDao

  private val daoUtility: DaoUtility = new DaoUtility(processLogDao)

  private val startTime = new Timestamp(System.currentTimeMillis())

  def getFileListToProcess(objectSummaryList: ListBuffer[S3ObjectSummary]): ListBuffer[String] = {
    var listOfFile:ListBuffer[String] = ListBuffer()
    for (index <- 0 to objectSummaryList.size-1){
      listOfFile += "s3n://"+ objectSummaryList(index).getBucketName+"/"+objectSummaryList(index).getKey
    }
    listOfFile
  }

  def generateSOR(objectSummaryList: ListBuffer[S3ObjectSummary], sparkSession: SparkSession, destinationLocationSORIncr: String, schema: StructType, hashValue: Long) = {

    /** * Declaring Constants for the system of Record ***/
    val nodeXProvider = "LV"
    val nodeXIdType = "INDL"
    val linkProvider = "LV"
    val linkProviderType = "DT"
    val nodeYProvider_NM = "NM"
    val nodeYProvider_MB = "MB"
    val nodeYIdType_HOUSEH = "HOUSEH"
    val nodeYIdType_COOKIE = "COOKIE"
    val nodeYIdType_DEVICE = "DEVICE"
    val feedType = "LV"
    val typeMerge = "merge"
    val typeDetach = "detach"

    val destinationLocationSOR = config.getString("spark.app.destination_dir")

    val bucketName = config.getString("aws.bucketName")
    val prefix = config.getString("aws.prefix")
    val awsCreds = new DefaultAWSCredentialsProviderChain()
    val s3Client = new AmazonS3Client(awsCreds)
    val objects = s3Client.listObjects(bucketName, prefix)

    /** * Read the LiveRamp Feed using Spark Streaming ***/
    val rawReader = sparkSession
      .read
      .option("fileNameOnly", true)
      .option("header", true)
      .option("delimiter", "\t")
      .option("mode", "FAILFAST")
      .schema(schema)
      .csv(getFileListToProcess(objectSummaryList): _*)

    /** * Partition the read data into smaller files using Hash File Format (txt / tab delimited ): DeviceID	Individual IDL	HouseholdIDL ***/
    import sparkSession.implicits._
    val repartition = rawReader.select("deviceid", "individualIDL", "householdIDL")
      .withColumn("partitionhash", hash($"individualIDL") % hashValue)
      .withColumn("filePath", input_file_name())

    val partitionedStream = repartition.repartition(hashValue.toInt, 'partitionhash)
    val dfWithoutPart = partitionedStream.select("deviceid", "individualIDL", "householdIDL", "filePath")

    def addDateAndTypeFromFilePath(data: String, delimiter: String, col: Int): String = {
      try {
        val splitVal = data.split(delimiter)
        val result = splitVal(splitVal.length - col)
        result
      } catch {
        case e: SparkException =>
          println(e.getLocalizedMessage)
          println(e.getSuppressed)
          null
        case e: Throwable =>
          println(e.getLocalizedMessage)
          println(e.getSuppressed)
          println(e.getCause)
          println(e.getStackTrace)
          e.printStackTrace()
          null
      }
    }

    val addDate = udf((data: String) => addDateAndTypeFromFilePath(data, "_", 2))
    val dfWithDate = dfWithoutPart.withColumn("getdate", addDate('filePath))
    val dfWithFormatDate = dfWithDate.withColumn("date", from_unixtime(unix_timestamp(dfWithDate.col("getdate"), "yyyyMMdd"), "yyyy-MM-dd"))

    val addDataType = udf((data: String) => addDateAndTypeFromFilePath(data, "_", 3))
    val dfWithType = dfWithFormatDate.withColumn("datasettype", addDataType('filePath))

    val addFileType = udf((data: String) => addDateAndTypeFromFilePath(data, "_", 4))
    val dfWithFileTypeValue = dfWithType.withColumn("filetype", addFileType('filePath))
    val dfWithFileType = dfWithFileTypeValue
      .withColumn("filetypeid", lit(2))
      .withColumn("destlocation", lit(destinationLocationSOR))

    val dfWithDateAndType = dfWithFileType
      .select("deviceid", "individualIDL", "householdIDL", "date", "datasettype", "filetypeid", "filepath", "destlocation")

    val dfAuditInsert = dfWithFileType
      .select("date", "filetypeid", "filepath", "destlocation").distinct()

    val processLogHolderList = dfAuditInsert.map(r => ProcessLogDF(r.getString(0), r.getInt(1), r.getString(2), r.getString(3))).collect().toList

    //Insert into Audit Log with Inprogress Status
    val processLogList: List[util.ArrayList[ProcessLogImpl]] = daoUtility.initializeProcessLog(processLogHolderList, IdrProcessStatus.IN_PROGRESS, startTime)


    try {
      /** * Concatenate IDL and HouseHold ID if Household ID is a MATCHED value ***/
      def concatIDLAndHLDID(deviceID: String, hldidl: String, delimiter: String): String = {
        try {
          if (hldidl == "UNMATCHED") {
            deviceID
          } else {
            deviceID + delimiter + hldidl
          }
        } catch {
          case e: SparkException =>
            println(e.getLocalizedMessage)
            println(e.getSuppressed)
            null
          case e: Throwable =>
            println(e.getLocalizedMessage)
            println(e.getSuppressed)
            println(e.getCause)
            println(e.getStackTrace)
            e.printStackTrace()
            null
        }
      }

      val concatID = udf((deviceID: String, hldidl: String) => concatIDLAndHLDID(deviceID, hldidl, "::"))
      val dfWithConcatID = dfWithDateAndType.withColumn("ConcatID", concatID('deviceid, 'householdIDL))

      /**
        * Explode the DataFrame on ConcatID columns
        * pre-explosion
        *
        * 214430f0  XYCZzSIqb hYhCZ4Oy3 20170821  Cookie  214430f0::hYhCZ4Oy3
        *
        * Post Explosion
        * 214430f0  XYCZzSIqb hYhCZ4Oy3 20170821  Cookie  214430f0
        * 214430f0  XYCZzSIqb hYhCZ4Oy3 20170821  Cookie  hYhCZ4Oy3
        *
        */
      val dfWithSplitID = dfWithConcatID.withColumn("ConcatID", explode(split($"ConcatID", "::")))

      /** * Adding additional constants to the DataFrame ***/
      val dfWithNodeCol = dfWithSplitID
        .withColumn("NodeXProvider", lit(nodeXProvider))
        .withColumn("NodeXIdType", lit(nodeXIdType))
        .withColumn("LinkProvider", lit(linkProvider))
        .withColumn("LinkProviderCustomVal", substring(dfWithSplitID.col("individualIDL"), 0, 2))
        .withColumn("LinkProviderType", lit(linkProviderType))
        .withColumnRenamed("ConcatID", "OrigNodeYIdVal")
        .withColumnRenamed("individualIDL", "OrigNodeXIdVal")

      /** * Add NodeYProvider and NodeYIdType ***/
      def addNodeYIdType(nodeYVal: String, fld: String, fileType: String): String = {
        try {
          if (fld == "provider") {
            if (nodeYVal.length == 49 && nodeYVal.startsWith("hY")) {
              nodeXProvider
            } else if (fileType == "Cookie") {
              nodeYProvider_NM
            } else {
              nodeYProvider_MB
            }
          } else if (fld == "datasettype") {
            if (nodeYVal.length == 49 && nodeYVal.startsWith("hY")) {
              nodeYIdType_HOUSEH
            } else if (fileType == "Cookie") {
              nodeYIdType_COOKIE
            } else {
              nodeYIdType_DEVICE
            }
          } else {
            null
          }
        } catch {
          case e: SparkException =>
            println(e.getLocalizedMessage)
            println(e.getSuppressed)
            null
          case e: Throwable =>
            println(e.getLocalizedMessage)
            println(e.getSuppressed)
            println(e.getCause)
            println(e.getStackTrace)
            e.printStackTrace()
            null
        }
      }

      val addNodeYType = udf((nodeYVal: String, fileType: String) => addNodeYIdType(nodeYVal, "datasettype", fileType))
      val addNodeYTypeVal = dfWithNodeCol.withColumn("NodeYIdType", addNodeYType('OrigNodeYIdVal, 'datasettype))

      val addNodeYProvider = udf((nodeYVal: String, fileType: String) => addNodeYIdType(nodeYVal, "provider", fileType))
      val dfWithAllCols = addNodeYTypeVal
        .withColumn("NodeYProvider", addNodeYProvider('OrigNodeYIdVal, 'datasettype))
        .withColumn("datafeed", lit(feedType))

      val dfConcartNodeIdVal = dfWithAllCols
        .withColumn("NodeXIdVal", concat(col("NodeXProvider"), col("OrigNodeXIdVal")))
        .withColumn("NodeYIdVal", concat(col("NodeYProvider"), col("OrigNodeYIdVal")))

      /** * Filtering the Household Ids and selecting the required columns into the DataFrame to create the system of record ***/
      val finalDF = dfConcartNodeIdVal.filter(!$"NodeYIdType".equalTo(nodeYIdType_HOUSEH)).select("NodeXProvider",
        "NodeXIdVal",
        "NodeXIdType",
        "LinkProvider",
        "LinkProviderType",
        "LinkProviderCustomVal",
        "NodeYProvider",
        "NodeYIdVal",
        "NodeYIdType",
        "date").distinct()


      /** * Filter opt-out rows from Incremental DF ***/
      val finalDFIncrOptout = finalDF.filter($"NodeXIdVal" === "LV0").withColumn("type", lit(typeDetach))
      val finalDFIncrSOR = finalDF.filter($"NodeXIdVal" =!= "LV0").withColumn("type", lit(typeMerge))


      /** * Write the Incremental system of Record to S3 bucket ***/
      if (!finalDFIncrSOR.head(1).isEmpty) {
        finalDFIncrSOR
          .repartition(hashValue.toInt)
          .write
          .partitionBy("date", "type")
          .option("header", true)
          .format("csv")
          .option("path", destinationLocationSORIncr)
          .mode(SaveMode.Append)
          .save()
      }

      /** * Write the Opt-outs to S3 bucket ***/
      if (!finalDFIncrOptout.head(1).isEmpty) {
        finalDFIncrOptout
          .repartition(5)
          .write
          .partitionBy("date", "type")
          .option("header", true)
          .format("csv")
          .option("path", destinationLocationSORIncr)
          .mode(SaveMode.Append)
          .save()
      }

      // Audit : Update Audit Store process with Success status
      daoUtility.updateProcessLog(processLogList, IdrProcessStatus.SUCCESS)

      //Delete all the processed files from staging location
      for (index <- 0 to objectSummaryList.size-1) {
        println("=====================================================")
        println(objectSummaryList(index).getKey)
        println("=====================================================")
        s3Client.deleteObject(objectSummaryList(index).getBucketName, objectSummaryList(index).getKey)
      }

    }
    catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e.getSuppressed)

        // Audit : Update Audit Store process with Failed status
        daoUtility.updateProcessLog(processLogList, IdrProcessStatus.FAILED)

        // Audit : Insert the process_detail_log table with error message
        daoUtility.insertProcessDetailLog(processLogList, e.getLocalizedMessage)

        null
    }

  }
}
