package com.migrate.co

import grails.validation.Validateable

@Validateable
class DiscountRuleActionCO {
    Double value
    String actionType

    static constraints = {
    }
}
