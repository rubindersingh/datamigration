package migration

import com.migrate.enums.ProgressStatus

class IndexFailureInfo {

    Long entityId
    String failureReason

    static belongsTo = [progressIndex : ProgressIndex]

    static constraints = {
        progressIndex unique: 'entityId'
    }

    static mapping = {
        failureReason sqlType: "text"
    }
}
