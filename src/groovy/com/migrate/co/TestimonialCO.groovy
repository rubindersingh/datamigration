package com.migrate.co

import grails.validation.Validateable

@Validateable
class TestimonialCO {
    String firstName
    String lastName
    String email
    String place
    String content

    static constraints = {
        firstName nullable: true, validator: {val, obj ->
            if(!val && !obj.properties["lastName"]) {
                return false
            } else {
                return true
            }
        }
        lastName nullable: true, validator: {val, obj ->
            if(!val && !obj.properties["firstName"]) {
                return false
            } else {
                return true
            }
        }
    }
}
