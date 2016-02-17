package com.migrate.co

import grails.validation.Validateable
import org.springframework.validation.ObjectError

@Validateable
class OrderItemCO {
    String id
    String itemStatus
    String productVariationId
    String productId
    String productName
    Integer qtyOrdered
    Integer qtyCancelled
    Integer qtyInvoiced
    Integer qtyShipped
    Double price
    Double taxAmount
    Double subTotal
    Double productDiscount
    Double couponDiscount
    Double taxPercentage
    String taxCategory
    ProductInfoCO productInfo
    List<ObjectError> errorCodes = []

    static constraints = {
        productInfo(nullable: false, validator: { val, obj ->
            if(!val.validate())
            {
                obj.errorCodes.addAll(val.errors.allErrors)
            }
            true
        })
    }
}
