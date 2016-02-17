package com.migrate.co

import grails.validation.Validateable

@Validateable
class ProductInfoCO {
    String name
    String sku
    String color
    String colorCode
    String description
    String premiumPackagingSKU
    String taxClass
    String size
    String shortDescription
    String eanNumber
    String discount
    Double price
    Double weight
    Long departmentId
    String department
    Long actualCategoryId
    String actualCategoryName
    Long baseCategoryId
    String baseCategoryName

    static constraints = {

    }
}
