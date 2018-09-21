package com.ignitionone.idrlr

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.ignitionone.idr.utilities.auditstore.{IdrProcess, IdrProcessDetailLogType, IdrProcessStatus}
import com.ignitionone.idr.utilities.auditstore.model.impl.{ProcessDetailLogImpl, ProcessLogImpl}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

class LRDataProcess {

  val config = globals.config
  private val processLogDao = globals.processLogDao
  private val processDetailLogDao = globals.processDetailLogDao
  private val daoUtility: DaoUtility = new DaoUtility(processLogDao)
  private val processDetailLog = new ProcessDetailLogImpl()
  private val formatter = new SimpleDateFormat("yyyyMMdd")
  private val cal = Calendar.getInstance().getTime
  private val currentDate = formatter.format(cal.getTime)
  var objectSummaryList: ListBuffer[S3ObjectSummary] = ListBuffer()
  var sizeOfAllFilesCombined = 0L
  private val desiredSizeOfEachFile = globals.sizeOfEachFile * 1024 * 1024

  def getNumberOfFilesToCreate(sizeOfAllFiles: Long) :Long = {
    Math.ceil(sizeOfAllFiles.toDouble/desiredSizeOfEachFile).toLong
  }

  def run() = {

    /** * Creating Source and Destination Location within Amazon S3 for the streaming process ***/
    val sourceLocation = config.getString("spark.app.source_dir")
    val s3DestinationPathSOR = config.getString("spark.app.destination_dir")
    val s3DestinationPathSORIncr = s3DestinationPathSOR + "LV/incr/"

    val bucketName = config.getString("aws.bucketName")
    val prefix = config.getString("aws.prefix")
    val awsCreds = new DefaultAWSCredentialsProviderChain()
    val s3Client = new AmazonS3Client(awsCreds)
    val noOfFilesPresent = s3Client.listObjects(bucketName, prefix).getObjectSummaries.size()
    val startTimeUTC = DateTime.now()

    /** * Calling Spark Generator to create Spark Session Object ***/
    val sparkSession = SparkSessionGenerator.GenerateSparkSession()
    try {
      /** * Creating the schema to read Liveramp file and Starting the streaming process ***/
      val lrRawSchema = ScalaReflection.schemaFor[LRRaw].dataType.asInstanceOf[StructType]
      if (noOfFilesPresent >= 1) {
        val listOfFileObjectSummaries = s3Client.listObjects(bucketName, prefix).getObjectSummaries
        for (index <- 0 to noOfFilesPresent-1){
          if (!listOfFileObjectSummaries.get(index).getKey.contains(currentDate)){
            objectSummaryList += listOfFileObjectSummaries.get(index)
            sizeOfAllFilesCombined = sizeOfAllFilesCombined + listOfFileObjectSummaries.get(index).getSize
          }
        }
        val hashValue=getNumberOfFilesToCreate(sizeOfAllFilesCombined)
        SORGenerator.generateSOR(objectSummaryList, sparkSession, s3DestinationPathSORIncr, lrRawSchema, hashValue)
      } else {
        println("=============================================================")
        println("=================== No File To Process ======================")
        println("=============================================================")
      }

    }
    catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println(e.getSuppressed)
        // Audit : Insert the process_log table with failed status
        val startTime = new Timestamp(System.currentTimeMillis())
        val log: ProcessLogImpl = daoUtility.initProcessLog(startTime.toString(), IdrProcess.INCREMENTAL_LIVERAMP_TO_SOR.getProcessId, "", "", IdrProcessStatus.FAILED, startTime, startTime)
        val processLogId = processLogDao.insertRow(log)

        // Audit : Insert the process_detail_log table with error message
        processDetailLog.setProcessLogId(processLogId)
        processDetailLog.setLogType(IdrProcessDetailLogType.ERROR)
        processDetailLog.setLogDescription(e.getLocalizedMessage)
        processDetailLogDao.insertRow(processDetailLog)
        null

    }
  }
}
