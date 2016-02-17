package com.migrate.co

import grails.validation.Validateable
import org.springframework.validation.ObjectError

@Validateable
class OrderCO {
    String id
    String orderId
    String orderStatus
    String paymentType
    String paymentProvider
    boolean isStoreCreditUsed
    String dcStatus
    String currency
    Double taxAmount
    Double subtotal
    Double grandTotal
    Double totalPaid
    Double shippingCharges
    List<OrderStatusCO> orderStatusLogList
    CouponDetailCO couponDetail
    AddressCO billingAddress
    AddressCO shippingAddress
    List<OrderItemCO> orderItems
    UserInfoCO userInfo
    List<TenderInfoCO> tenderInfoList
    List<ObjectError> errorCodes = []

    static constraints = {
        orderStatusLogList validator: {val, obj, errors->
            val.each {element ->
                if(!element.validate())
                {
                    obj.errorCodes.addAll(element.errors.allErrors)
                }
            }
            true
        }
        couponDetail nullable: true, validator: { val, obj ->
            if(val && !val.validate())
            {
                obj.errorCodes.addAll(val.errors.allErrors)
            }
            true
        }
        billingAddress validator: { val, obj ->
            if(!val.validate())
            {
                obj.errorCodes.addAll(val.errors.allErrors)
            }
            true
        }
        shippingAddress validator: { val, obj ->
            if(!val.validate())
            {
                obj.errorCodes.addAll(val.errors.allErrors)
            }
            true
        }
        orderItems validator: {val, obj ->
            val.each {element ->
                if(!element.validate())
                {
                    obj.errorCodes.addAll(element.errors.allErrors)
                    obj.errorCodes.addAll(element.errorCodes)
                }
            }
            true
        }
        userInfo validator: { val, obj ->
            if(!val.validate())
            {
                obj.errorCodes.addAll(val.errors.allErrors)
            }
            true
        }
        tenderInfoList validator: {val, obj ->
            val.each {element ->
                if(!element.validate())
                {
                    obj.errorCodes.addAll(element.errors.allErrors)
                }
            }
            true
        }

    }
}
