package com.migrate.co

import grails.validation.Validateable

@Validateable
class TenderInfoCO {
    String paymentProvider
    Double amount

    static constraints = {
        paymentProvider(nullable: false, blank: false)
        amount(nullable: false, blank: false)
    }
}
