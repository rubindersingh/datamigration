package com.migrate.co

import grails.validation.Validateable

@Validateable
class UserInfoCO {
    String emailId
    String firstName
    String mobileNumber
    Date dateOfBirth

    static constraints = {
        emailId(nullable: false, blank: false)
        firstName(nullable: false, blank: false)
        mobileNumber(nullable: false, blank: false)
        dateOfBirth(nullable: false, blank: false)
    }
}
