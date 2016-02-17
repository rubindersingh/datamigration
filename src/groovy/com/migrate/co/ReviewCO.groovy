package com.migrate.co

import grails.validation.Validateable

@Validateable
class ReviewCO {
    String id
    String userName
    String status
    Integer rating
    String location
    String title
    String comment
    String email
    String userType
    String productName
    String productSKU

    static constraints = {
        id(nullable: false, blank: false)
        userName(nullable: false, blank: false)
        status(nullable: false, blank: false)
        rating(nullable: false, blank: false)
        location(nullable: false, blank: false)
        title(nullable: false, blank: false)
        comment(nullable: true, blank: true)
        email(nullable: false, blank: false)
        userType(nullable: false, blank: false)
        productName(nullable: false, blank: false)
        productSKU(nullable: false, blank: false)
    }
}
