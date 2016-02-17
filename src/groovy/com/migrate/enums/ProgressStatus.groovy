package com.migrate.enums

enum ProgressStatus {

    DONE("Done"),
    IN_PROGRESS("In Progress"),
    ERROR("Error"),
    PAUSED("Paused")

    String value

    ProgressStatus(String value) {
        this.value = value
    }

}