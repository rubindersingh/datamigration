package com.migrate.co

import grails.validation.Validateable

@Validateable
class CategoryCO {
    String name
    Boolean isActive
    Boolean isVisible

    static constraints = {
    }
}
