package com.ignitionone.idrlr

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import com.ignitionone.idr.utilities.auditstore.{IdrProcess, IdrProcessDetailLogType, IdrProcessStatus}
import com.ignitionone.idr.utilities.auditstore.dao.ProcessLogDao
import com.ignitionone.idr.utilities.auditstore.model.impl.{ProcessDetailLogImpl, ProcessLogImpl}

class DaoUtility(processLogDao: ProcessLogDao) {

  private val processDetailLogDao = globals.processDetailLogDao
  private val processDetailLog = new ProcessDetailLogImpl()

  /**
    * Initializes ProcessLog
    *
    * @return ProcessLogImpl
    */

  def initProcessLog(effectiveDate: String, processId: Int, sourceLocation: String, destinationLocation: String, process_status: IdrProcessStatus, startTime: Timestamp, endTime: Timestamp): ProcessLogImpl = {
    val effectiveTimeStamp: Timestamp = new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse(effectiveDate).getTime)
    new ProcessLogImpl(0, sourceLocation, destinationLocation, 0, 0,
      effectiveTimeStamp, startTime, endTime, 0, IdrProcess.getById(processId),
      process_status, 0)
  }

  /**
    * Initializes ProcessLog with In progress Status
    *
    * @return List[ProcessLogImpl]
    */
  def initializeProcessLog(processLogHolderList: List[ProcessLogDF], process_status: IdrProcessStatus, startTime: Timestamp): List[util.ArrayList[ProcessLogImpl]] = {
    val processLogList = new util.ArrayList[ProcessLogImpl]
    processLogHolderList.map(r => {
      val sourceLoc = r.sourceLoc
      val effectiveDate = r.effectiveDate
      val processId = r.processId
      val destinationLoc = r.destinationLoc
      val log: ProcessLogImpl = initProcessLog(effectiveDate, processId, sourceLoc, destinationLoc, process_status, startTime, startTime)
      val processLogId = processLogDao.insertRow(log)
      log.setProcessLogId(processLogId)
      processLogList.add(log)
      processLogList
    })
  }

  /**
    * Update ProcessLog with final Status
    *
    */

  def updateProcessLog(processLogList: List[util.ArrayList[ProcessLogImpl]], process_status: IdrProcessStatus) = {
    processLogList.map(logList => {
      for (i <- 0 to logList.size() - 1) {
        val log = logList.get(i)
        log.setProcessStatusId(process_status)
        log.setEndTimeUtc(new Timestamp(System.currentTimeMillis()))
        processLogDao.updateRow(log)
      }
    })
  }

  /**
    * Insert Row into Process Detail Log with error message
    *
    */

  def insertProcessDetailLog(processLogList: List[util.ArrayList[ProcessLogImpl]], errorMessage: String) = {
    for (i <- 0 to processLogList.size - 1) {
      val log = processLogList(i).get(i)
      processDetailLog.setProcessLogId(log.getProcessLogId)
      processDetailLog.setLogType(IdrProcessDetailLogType.ERROR)
      processDetailLog.setLogDescription(errorMessage)
      processDetailLogDao.insertRow(processDetailLog)
    }
  }
}