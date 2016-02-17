package migration

import com.migrate.enums.ProgressStatus

import java.sql.Timestamp

class ProgressIndex {

    ProgressStatus status = ProgressStatus.IN_PROGRESS
    Long totalDocuments = 0
    Long countProcessed = 0
    Long countMigrated = 0
    Long resumePoint = 0
    String indexName = ""
    Boolean isCurrent = true
    Long countValid = 0
    Long countFail = 0
    Long countIterationValid = 0
    Long countIterationFail = 0
    String bulkErrorMessage = ""
    Timestamp progressStart
    Timestamp progressEnd

    static belongsTo = [progress : Progress]

    static constraints = {
        progressEnd nullable: true
        progressStart nullable: true
    }

    static mapping = {
        bulkErrorMessage sqlType: "longtext"
    }
}
