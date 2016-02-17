package com.migrate.co

import grails.validation.Validateable

@Validateable
class CouponCO {
    String ruleName
    String couponActionType
    Double value

    static constraints = {
        //value min: 1D
    }
}
