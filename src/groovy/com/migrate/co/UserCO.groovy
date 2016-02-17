package com.migrate.co

import grails.validation.Validateable

@Validateable
class UserCO {
    String id
    String emailId
   /* String gender
    String mobileNumber*/

    static constraints = {
        emailId email: true
    }
}
