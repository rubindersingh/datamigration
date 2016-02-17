package com.migrate.co

import grails.validation.Validateable

@Validateable
class AddressCO {
    String id
    String firstName
    String mobileNumber
    String city
    String pinCode
    String country
    String state
    String addressLine1

    static constraints = {
        id(nullable: false, blank: false)
        firstName(nullable: false, blank: false)
        mobileNumber(nullable: false, blank: false)
        city(nullable: false, blank: false)
        pinCode(nullable: false, blank: false)
        country(nullable: false, blank: false)
        state(nullable: false, blank: false)
        addressLine1(nullable: false, blank: false)
    }
}
