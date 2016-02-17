package com.migrate

import org.apache.commons.logging.LogFactory
import org.hibernate.SQLQuery
import org.hibernate.Session
import org.hibernate.SessionFactory
import org.hibernate.transform.AliasToEntityMapResultTransformer

class CommonService {

    static SessionFactory sessionFactory
    static Map attributeMap = [:]
    static Map genderMap = [:]
    static Map brandMap = [:]
    static Map discountMap = [:]
    static Map colorMap = [:]
    static Map colorNameMap = [:]
    static Map colorCodeMap = [:]
    static Map seasonMap = [:]
    static Map sizeMap = [:]
    static List personalizationConfiguredCategoryList = []
    static Map resolutionMap = [
            "3": "EXCHANGE",
            "4": "REFUND",
            "5": "REFUND",
    ]
    static Map conditionMap = [
            "6": "UNOPENED",
            "7": "OPENED",
            "8": "DAMAGED",
    ]
    static Map reasonMap = [
            "9" : "COLOR",
            "10": "SIZE",
            "11": "QUALITY",
            "0" : "OTHER",
    ]

    static Map rmaStatusMap = [
            "pending"   : "CREATED",
            "authorized": "ACCEPTED",
            "denied"    : "CREATED",
            "closed"    : "ACCEPTED",
    ].withDefault { "CREATED" }

    static Map countryMap = [
            IN: "India",
            US: "United States",
            GB: "United Kingdom",
            ES: "Spain",
            DE: "Germany",
    ]
    static Map taxClassMap = [:]


    static Map orderStatusMap = [
            canceled                    : "CANCELLED",
            confirmed_by_warehouse      : 'CONFIRMED_BY_WAREHOUSE',
            pending_payment             : 'PENDING',
            COD_Verification_Successful : 'CREATED',
            delivered                   : 'DELIVERED',
            created                     : 'CREATED',
            closed                      : 'CLOSED',
            shipped                     : 'SHIPPED',
            order_unsuccessful          : 'CANCELLED',
            processing                  : 'PROCESSING',
            complete                    : 'COMPLETED',
            not_delivered               : 'SHIPPED',
            holded                      : 'PENDING',
            //NULL                        : 'CLOSED',
            "1"                         : 'CLOSED',
            COD_Verification_Pending    : 'PENDING',
            COD_Verification_Unsucessful: 'CANCELLED',
            pending                     : 'PENDING',
            processed_ogone             : 'PROCESSING',
    ]

    static Map orderDCStatusMap = [
            "0": "NOT_SENT_TO_DC",
            "1": 'SENT_TO_DC',
            "2": 'CONFIRMED',
            "3": 'REJECTED',
            "4": 'SHIPPED',
            "5": 'DELIVERED',
            "6": 'CONFIRMED',
            "7": 'SHIPPED',
    ]

    static Map paymentTypeMap = [
            zaakpay            : "PREPAID",
            cashondelivery     : 'COD',
            payseal_standard   : 'PREPAID',
            free               : 'STORE_CREDIT',
            ccavenuepay        : 'PREPAID',
            payucheckout_shared: 'PREPAID',
            paytm_cc           : 'PREPAID',
            payumoney_shared   : 'PREPAID',
    ]

    static Map paymentProviderMap = [
            zaakpay            : "ZAAKPAY",
            cashondelivery     : 'CASH_ON_DELIVERY',
            payseal_standard   : 'PAYSEAL_STANDARD',
            free               : 'STORE_CREDIT',
            ccavenuepay        : 'CC_AVENUE',
            payucheckout_shared: 'PAYUMONEY_WALLET',
            paytm_cc           : 'PAYTM_WALLET',
            payumoney_shared   : 'PAYUMONEY_WALLET',
    ]

    static Map customerGenderMap = [
            "1": "Male",
            "2": 'Female',
    ]

    static Map statusMap = [
            "1": "APPROVED",
            "2": "NONAPPROVED",
            "3": "PENDING"
    ]

    static Map utmMap = [
            "251": { String value -> ["source": value] },
            "261": { String value -> ["campaign": value] },
    ].withDefault {
        { String value -> [:] }
    }

    static Map actionConditionMap = [
            "by_percent"        : "PERCENT_OF_PRODUCT_PRICE_DISCOUNT",
            "by_fixed"          : "FIXED_AMOUNT_DISCOUNT",
            "cart_fixed"        : "FIXED_AMOUNT_DISCOUNT_FOR_WHOLE_CART",
            "the_cheapest"      : "PERCENT_DISCOUNT_FOR_THE_CHEAPEST",
            "the_most_expencive": "PERCENT_DISCOUNT_FOR_THE_MOST_EXPENSIVE"
    ]

    static Map creditMemoStatusMap = [
            "1": 'PENDING',
            "2": 'REFUNDED',
            "3": 'CANCELED',
    ]

    static List getAttributeOptionValue(Integer attributeId) {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("SELECT eaov.option_id, eaov.value\n" +
                "        FROM eav_attribute_option_value eaov\n" +
                "        LEFT JOIN catalog_product_entity_int cpei ON eaov.option_id=cpei.value\n" +
                "        WHERE attribute_id = :attribute_id GROUP BY eaov.value;")
        def optionValues = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("attribute_id", attributeId)
            list()
        }
        hSession.close()

        optionValues
    }

    static List getColorValues() {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("""SELECT eao.option_id,eaov.value FROM eav_attribute_option eao LEFT JOIN eav_attribute_option_value eaov ON
        eao.option_id = eaov.option_id WHERE eao.attribute_id=226 AND eaov.store_id=0""")
        def optionValues = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            list()
        }
        hSession.close()

        optionValues
    }

    static List getColorNameValues() {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("""SELECT eao.option_id,eaov.value FROM eav_attribute_option eao LEFT JOIN eav_attribute_option_value eaov ON
        eao.option_id = eaov.option_id WHERE eao.attribute_id=92 AND eaov.store_id=1""")
        def optionValues = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            list()
        }
        hSession.close()

        optionValues
    }

    static List getColorCodeValues() {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("""SELECT eao.option_id,eaov.value FROM eav_attribute_option eao LEFT JOIN eav_attribute_option_value eaov ON
        eao.option_id = eaov.option_id WHERE eao.attribute_id=92 AND eaov.store_id=0""")
        def optionValues = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            list()
        }
        hSession.close()

        optionValues
    }

    static List getTaxClasses() {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("SELECT class_id,class_name FROM tax_class")
        def taxClasses = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            list()
        }
        hSession.close()

        taxClasses
    }

    static void prepareMasterData() {

        if (!personalizationConfiguredCategoryList) {
            personalizationConfiguredCategoryList = getPersonalizationConfiguredCategories()
            personalizationConfiguredCategoryList << "260"
            personalizationConfiguredCategoryList << "283"
        }

        if (!genderMap)
            getAttributeOptionValue(211).each { optionvalue ->
                genderMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!brandMap)
            getAttributeOptionValue(242).each { optionvalue ->
                brandMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!discountMap)
            getAttributeOptionValue(220).each { optionvalue ->
                discountMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!colorNameMap)
            getColorNameValues().each { optionvalue ->
                colorNameMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!colorMap)
            getColorValues().each { optionvalue ->
                colorMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!colorCodeMap)
            getColorCodeValues().each { optionvalue ->
                colorCodeMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!seasonMap)
            getAttributeOptionValue(188).each { optionvalue ->
                seasonMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!sizeMap)
            getAttributeOptionValue(175).each { optionvalue ->
                sizeMap[optionvalue['option_id'].toString()] = optionvalue['value']
            }

        if (!taxClassMap)
            getTaxClasses().each { taxClass ->
                taxClassMap[taxClass['class_id'].toString()] = taxClass['class_name']
            }

        attributeMap = CommonUtils.mapForAttribute

        attributeMap << ["92": { String value -> [["colorName": colorNameMap[value]], ["colorCode": colorCodeMap[value]]] }]
        attributeMap << ["226": { String value -> ["color": colorMap[value]] }]
        attributeMap << ["175": { String value -> ["size": sizeMap[value]] }]
        attributeMap << ["188": { String value -> ["seasonCode": seasonMap[value]] }]
        attributeMap << ["211": { String value -> ["gender": genderMap[value]] }]
        attributeMap << ["220": { String value ->
            Integer discount
            try {
                discount = discountMap[value].toString().toInteger()
            } catch (Exception ignored) {
                discount = null
            }
            ["discount": discount]
        }]
        attributeMap << ["242": { String value -> ["brand": brandMap[value]] }]
        attributeMap << ["27": { String value -> ["country": countryMap[value]] }]
        attributeMap << ["122": { String value -> ["taxClass": taxClassMap[value]] }]
        attributeMap << ["159": { String value -> ["returnResolution": resolutionMap[value]] }]
        attributeMap << ["160": { String value -> ["returnItemCondition": conditionMap[value]] }]
        attributeMap << ["161": { String value -> ["returnReason": reasonMap[value]] }]

    }

    static Map getMapForAttribute() {
        attributeMap
    }

    static Map addAttributes(Map responseMap, List attributes) {
        attributes.each { attribute ->
            def object = getMapForAttribute()[attribute["attribute_id"].toString()](attribute["value"]?.toString())
            if (object instanceof List) {
                object.each {
                    responseMap << it
                }
            } else {
                responseMap << object
            }

        }
        responseMap
    }

    static Map addUserUTMAttributes(Map responseMap, List attributes) {
        attributes.each { attribute ->
            def object = utmMap[attribute["attribute_id"].toString()](attribute["value"]?.toString())
            responseMap << object
        }
        responseMap
    }

    static List getPersonalizationConfiguredCategories() {
        Session hSession = sessionFactory.openSession()
        SQLQuery query = hSession.createSQLQuery("SELECT DISTINCT entity_id FROM catalog_category_entity WHERE parent_id IN(260,283)")
        List categoryIds = query.with {
            list()
        }.collect { it.toString() }
        hSession.close()

        categoryIds
    }
}
