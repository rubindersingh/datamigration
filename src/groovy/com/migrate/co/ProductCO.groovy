package com.migrate.co

import grails.validation.Validateable

@Validateable
class ProductCO {
    List<Map> productCategoryInformations
    String sku
    String name
    String price
    String taxClass
    String gender

    static constraints = {
        //productCategoryInformations minSize: 1
    }
}
