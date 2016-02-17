package com.migrate.co

import grails.validation.Validateable

@Validateable
class CouponDetailCO {
    String couponId
    String couponCode
    Double couponValue

    static constraints = {
        couponId(nullable: false, blank: false)
        couponCode(nullable: false, blank: false)
        couponValue(nullable: false, blank: false)
    }
}
