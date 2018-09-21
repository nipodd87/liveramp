package com.ignitionone.idrlr

import java.sql.Timestamp


case class LRRaw(deviceId: String, individualIDL: String, householdIDL: String)

case class ProcessLog(processLogId: Int, sourceLocation: String, destinationLocation: String, sourceRecordCount: Int,
                      destinationRecordCount: Int, effectiveDateUTC: Timestamp, startTimeUTC: Timestamp, endTimeUTC: Timestamp,
                      processId: Int, processStatusId: Int, parentProcessLogId: Int, processInstanceId: Int)

case class ProcessDetailLog(processDetailLogId: Int, processLogId: Int, logType: Int, logDescription: String)

case class ProcessLogDF(effectiveDate: String, processId: Int, sourceLoc: String, destinationLoc: String)