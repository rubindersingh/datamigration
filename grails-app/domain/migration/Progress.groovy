package migration

class Progress {

    Date dateCreated
    Date lastUpdated

    static hasMany = [progressIndexes: ProgressIndex]

    static constraints = {

    }

}
