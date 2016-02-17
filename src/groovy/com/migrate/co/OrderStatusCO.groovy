package com.migrate.co

import grails.validation.Validateable

@Validateable
class OrderStatusCO {
    String id
    String orderStatus
    Boolean isCustomerNotified

    static constraints = {
        id(nullable: false, blank: false)
        orderStatus(nullable: false, blank: false)
        isCustomerNotified(nullable: false, blank: false)
    }
}
