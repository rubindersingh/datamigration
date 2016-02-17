package com.migrate

import com.migrate.co.AddressCO
import com.migrate.co.CouponDetailCO
import com.migrate.co.OrderCO
import com.migrate.co.OrderItemCO
import com.migrate.co.OrderStatusCO
import com.migrate.co.ProductCO
import com.migrate.co.ProductInfoCO
import com.migrate.co.TenderInfoCO
import com.migrate.co.UserCO
import com.migrate.co.UserInfoCO
import com.migrate.enums.ProgressStatus
import com.migrate.exception.MigrationException
import groovy.transform.TailRecursive
import migration.Progress
import migration.ProgressIndex
import org.codehaus.groovy.grails.web.binding.DataBindingUtils
import org.hibernate.SQLQuery
import org.hibernate.Session
import org.hibernate.SessionFactory
import org.hibernate.exception.ConstraintViolationException
import org.hibernate.transform.AliasToEntityMapResultTransformer

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class OrderService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    CommonService commonService
    def messageSource
    static volatile int processed = 0
    private static final Object processedLock = new Object();
    static volatile int failed = 0
    private static final Object failedLock = new Object();
    static volatile int succeeded = 0
    private static final Object succeededLock = new Object();

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Order migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long orderCount = hSession.createSQLQuery("SELECT count(entity_id) FROM sales_flat_order").list().first() as Long
            Long timeStamp = new Date().time
            log.info(orderCount + " many orders are found to migrate")

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='ORDER'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < orderCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + orderCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", orderCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }


            } else {
                log.info("Running fresh migration for Orders")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "ORDER")
                    setLong("total_documents", orderCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='ORDER'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()
            if (counter < orderCount) {
                populateOrder(progressId, counter, orderCount, timeStamp, hSession, progressIndexId)

                query = hSession.createSQLQuery("""UPDATE progress_index
                SET progress_end =:progress_end, status =:status, is_current =:is_current WHERE id =:id""")
                query.with {
                    setTimestamp("progress_end", new Date())
                    setString("status", ProgressStatus.DONE.toString())
                    setBoolean("is_current", false)
                    setLong("id", progressIndexId)
                    executeUpdate()
                }
            }


            log.info("-----------------------------Ending Order migration--------------------------------------------")
        }
        catch (Exception e) {
            throw e
        }
        finally {
            if (hSession) {
                hSession.flush()
                hSession.close()
            }
        }
    }

    @TailRecursive
    Boolean populateOrder(Long progressId, Long counter, Long orderCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping order migration-------------------------------------------")
            MigrateJob.isInterrupted = false
            SQLQuery query1 = hSession.createSQLQuery("""UPDATE progress_index
                SET status =:status WHERE id =:id""")
            query1.with {
                setString("status", ProgressStatus.PAUSED.toString())
                setLong("id", progressIndexId)
                executeUpdate()
            }
            throw new MigrationException("Migration is interrupted")
        }

        if (counter < orderCount) {
            processed = 0
            failed = 0
            succeeded = 0

            Date date1 = new Date()
            List orders = getOrdersFromDb(counter, hSession)
            log.info("Fetched " + orders.size() + " many orders from db to migrate in one iteration")
            List<Map> orderList = [].asSynchronized()
            List<Map> invoiceList = [].asSynchronized()
            List<Map> shipmentList = [].asSynchronized()
            List<Map> creditMemoList = [].asSynchronized()
            List<Map> rmaList = [].asSynchronized()
            List<Map> orderCouponList = [].asSynchronized()

            String errorTrackingMessage = null

            def orderCallable = { order, hSessionInner ->

                try {

                    /*Capturing Order status logs Info*/
                    List orderStatusLogs = getOrderStatusLogsFromDb(order["entity_id"], hSessionInner)
                    List<Map> orderStatusLogList = []
                    orderStatusLogs.each { orderStatusLog ->

                        Map orderStatusLogMap = [
                                id                : orderStatusLog["entity_id"].toString(),
                                orderStatus       : orderStatusLog["status"] ? CommonService.orderStatusMap[orderStatusLog["status"]] : 'CLOSED',
                                isCustomerNotified: orderStatusLog["is_customer_notified"] == 1 ? true : false,
                                message           : orderStatusLog["comment"],
                                source            : "AS",
                                dateCreated       : orderStatusLog["created_at"] ? orderStatusLog["created_at"].time : timeStamp,
                                lastUpdated       : orderStatusLog["created_at"] ? orderStatusLog["created_at"].time : timeStamp,
                                isDeleted         : false,
                                lastUpdatedBy     : "",
                                messageTrackingId : null,
                        ]

                        orderStatusLogList << orderStatusLogMap
                    }

                    Map couponDetail = [:]
                    if (order["applied_rule_ids"]) {
                        Long couponId = null;
                        List appliedCouponIds = order["applied_rule_ids"].toString().tokenize(",")
                        if (appliedCouponIds.size() == 1) {
                            couponId = appliedCouponIds[0].trim().toLong()
                        } else if (appliedCouponIds.size() > 1) {
                            couponId = appliedCouponIds[1].trim().toLong()
                        }

                        if (couponId) {
                            couponDetail = [
                                    couponId        : couponId.toString(),
                                    couponCode      : order["coupon_code"],
                                    couponValue     : order["discount_amount"]?.abs(),
                                    couponRuleinfo  : null,
                                    couponRuleAction: null,
                            ]

                            Map coupon = getCouponFromDb(couponId, hSessionInner)
                            if (coupon) {

                                def couponType = null
                                if (coupon["coupon_type"] == 1) {
                                    couponType = 'NO_COUPON'
                                } else if (coupon["coupon_type"] == 2 && coupon["use_auto_generation"] == 0) {
                                    couponType = 'SPECIFIC_COUPON'
                                } else {
                                    couponType = 'AUTO'
                                }

                                Map couponRuleinfo = [
                                        ruleName        : coupon["name"],
                                        description     : coupon["description"],
                                        fromDate        : coupon["from_date"] ? CommonUtils.convertTimeStamptoDate(coupon["from_date"]) : null,
                                        toDate          : coupon["to_date"] ? CommonUtils.convertTimeStamptoDate(coupon["to_date"]) : null,
                                        status          : coupon["is_active"] == 1,
                                        priority        : coupon["sort_order"],
                                        couponType      : couponType,
                                        voucherType     : coupon["voucher_type"].toString().toUpperCase(),
                                        recurringOn     : [],
                                        defaultRuleLabel: coupon["label"],
                                        usesPerCoupon   : coupon["uses_per_coupon"],
                                        usesPerCustomer : coupon["uses_per_customer"],
                                        days            : null,
                                        hours           : null,
                                        minutes         : null,
                                        dateOrDuration  : true,
                                        activationDate  : coupon["from_date"] ? coupon["from_date"].time : null,
                                ]

                                couponDetail["couponRuleinfo"] = couponRuleinfo

                                Map couponRuleAction = [
                                        couponActionType   : CommonService.actionConditionMap[coupon["simple_action"]],
                                        value              : coupon["discount_amount"] as Double,
                                        discountIsAppliedTo: coupon["discount_qty"] as Integer,
                                        freeShipping       : coupon["simple_free_shipping"] == 1 ? 'YES' : 'NO',
                                        lineItemFilterNode : null,
                                        useProductMRP      : false,
                                ]

                                couponDetail["couponRuleAction"] = couponRuleAction
                            }

                            Map orderCouponMap = [
                                    id           : couponId.toString(),
                                    couponId     : couponId.toString(),
                                    couponCode   : order["coupon_code"],
                                    orderId      : order["entity_id"].toString(),
                                    userId       : order["customer_id"]?.toString(),
                                    dateCreated  : order["created_at"] ? order["created_at"].time : timeStamp,
                                    lastUpdated  : order["updated_at"] ? order["updated_at"].time : timeStamp,
                                    isDeleted    : false,
                                    lastUpdatedBy: null,
                            ]
                            orderCouponList << orderCouponMap
                        }
                    }

                    List orderItems = getOrderItemsFromDb(order["entity_id"], hSessionInner)

                    def orderReturnStatus = null
                    int returnCount = 0
                    int partialReturnCount = 0
                    int itemsCount = orderItems.size()
                    def taxCode = getOrderTaxCategoryFromDb(order["entity_id"], hSessionInner)
                    def taxCategory = ""

                    if (taxCode) {
                        if (taxCode.toString().startsWith("CST")) {
                            taxCategory = "CST"
                        }
                        if (taxCode.toString().startsWith("VAT")) {
                            taxCategory = "VAT"
                        }
                    } else {
                        if (ZipcodeConstants.haryanaCodes.containsKey(order["shipping_postcode"])) {
                            taxCategory = "VAT"
                        } else {
                            taxCategory = "CST"
                        }


                    }


                    List<Map> orderItemList = []
                    orderItems.each { orderItem ->

                        def prodVariationId = orderItem["product_var_id"] ? orderItem["product_var_id"] : orderItem["product_id"]
                        def sku = getProductSkuFromDb(orderItem["product_id"], hSessionInner)
                        List productAttributes = getProductAttributes(orderItem["product_id"], hSessionInner)
                        List productVariationAttributes = getProductAttributes(prodVariationId, hSessionInner)
                        Map productInfoDB = [:]
                        productInfoDB = commonService.addAttributes(productInfoDB, productAttributes)
                        Map productVariationInfoDB = [:]
                        productVariationInfoDB = commonService.addAttributes(productVariationInfoDB, productVariationAttributes)

                        Map productInfo = [
                                name               : productInfoDB["name"],
                                sku                : sku,
                                color              : productInfoDB["color"],
                                colorCode          : productVariationInfoDB["colorCode"],
                                description        : productInfoDB["description"],
                                premiumPackagingSKU: productInfoDB["premiumPackagingSKU"],
                                taxClass           : productInfoDB["taxClass"],
                                size               : productVariationInfoDB["size"],
                                shortDescription   : productVariationInfoDB["shortDescription"],
                                eanNumber          : productVariationInfoDB["eanNumber"],
                                discount           : productVariationInfoDB["discount"] ?: 0,
                                price              : productInfoDB["price"],
                                weight             : productVariationInfoDB["weight"],
                                volumetric_weight  : null, //change required here
                                departmentId       : null,
                                department         : null,
                                actualCategoryId   : null,
                                actualCategoryName : null,
                                baseCategoryId     : null,
                                baseCategoryName   : null,
                        ]

                        List categoryIds = []
                        Map categoryInfo = getProductCategoryInfoFromDb(orderItem["product_id"], hSessionInner);
                        if (categoryInfo) {
                            Long department = null
                            if (productInfoDB["department"]) {
                                department = Long.parseLong(productInfoDB["department"])
                                categoryIds << department
                            }

                            Long actualCategory = null
                            if (categoryInfo["category_id"] && categoryInfo["category_id"] != 1 && categoryInfo["category_id"] != 2) {
                                actualCategory = categoryInfo["category_id"]
                                categoryIds << actualCategory
                            }

                            Long baseCategory = null
                            if (categoryInfo["path"] && categoryInfo["path"].length() > 4) {
                                baseCategory = Long.parseLong(categoryInfo["path"].tokenize("/")[2])
                                categoryIds << baseCategory
                            }
                            if (categoryIds) {
                                List categoryNames = getCategoryNamesFromDb(categoryIds, hSessionInner)
                                categoryNames.each { categoryName ->
                                    if (categoryName["entity_id"] == department) {
                                        productInfo["departmentId"] = department.toString()
                                        productInfo["department"] = categoryName["value"]
                                    }

                                    if (categoryName["entity_id"] == actualCategory) {
                                        productInfo["actualCategoryId"] = actualCategory.toString()
                                        productInfo["actualCategoryName"] = categoryName["value"]
                                    }

                                    if (categoryName["entity_id"] == baseCategory) {
                                        productInfo["baseCategoryId"] = baseCategory.toString()
                                        productInfo["baseCategoryName"] = categoryName["value"]
                                    }
                                }
                            }

                        }

                        def itemReturnStatus = null
                        if (orderItem["qty_refunded"] > 0) {
                            if (orderItem["qty_refunded"] == orderItem["qty_ordered"]) {
                                itemReturnStatus = 'RETURNED'
                                returnCount++
                            } else if (orderItem["qty_refunded"] < orderItem["qty_ordered"]) {
                                itemReturnStatus = 'PARTIAL_RETURNED'
                                partialReturnCount++
                            }
                        }

                        Float productMRP = orderItem["product_mrp"] ? orderItem["product_mrp"] : productInfo["price"] ? productInfo["price"] : orderItem["original_price"]

                        Map orderItemMap = [
                                id                  : orderItem["item_id"].toString(),
                                itemStatus          : order["status"] ? CommonService.orderStatusMap[order["status"]] : 'CLOSED',
                                productVariationId  : orderItem["sku"].toString(),//prodVariationId.toString(),
                                productId           : orderItem["product_id"].toString(),
                                productName         : orderItem["name"],
                                qtyOrdered          : orderItem["qty_ordered"] ? orderItem["qty_ordered"] as int : 0,
                                qtyCancelled        : orderItem["qty_canceled"] ? orderItem["qty_canceled"] as int : 0,
                                qtyInvoiced         : orderItem["qty_invoiced"] ? orderItem["qty_invoiced"] as int : 0,
                                qtyShipped          : orderItem["qty_shipped"] ? orderItem["qty_shipped"] as int : 0,
                                stockQuantity       : null, //why we need stock quantity
                                price               : productMRP,
                                taxAmount           : orderItem["row_total_incl_tax"] ? (orderItem["row_total_incl_tax"] - orderItem["row_total"]) : 0.0,//for all
                                taxCategory         : orderItem["code"],
                                taxPercentage       : orderItem["tax_percent"],
                                subTotal            : (orderItem["row_total_incl_tax"] ? orderItem["row_total_incl_tax"] : 0.0) - (orderItem["discount_amount"] ? orderItem["discount_amount"] : 0.0),
                                productDiscount     : (productMRP - orderItem["original_price"]) * orderItem["qty_ordered"], //for all
                                couponDiscount      : orderItem["discount_amount"]?.abs(), //for all
                                expectedDeliveryDays: null, //not maintained by magento
                                itemReturnStatus    : itemReturnStatus,
                                productInfo         : productInfo,

                        ]

                        orderItemList << orderItemMap
                    }

                    //Calculating order return status from items return status
                    if (partialReturnCount > 0) {
                        orderReturnStatus = 'PARTIAL_RETURNED'
                    } else if (returnCount > 0) {
                        if (returnCount == itemsCount) {
                            orderReturnStatus = 'RETURNED'
                        } else {
                            orderReturnStatus = 'PARTIAL_RETURNED'
                        }
                    }

                    /*Capturing Billing Address Info*/
                    List billingAddressLines = order["billing_street"].toString().tokenize("\n")
                    def billingAddressLine1 = billingAddressLines[0]
                    def billingAddressLine2 = null
                    if (billingAddressLines.size() > 1) {
                        billingAddressLines.remove(0)
                        billingAddressLine2 = billingAddressLines.join("\n")
                    }

                    Map billingAddress = [
                            id           : order["billing_entity_id"].toString(),
                            nickName     : '',
                            title        : order["billing_prefix"],
                            firstName    : order["billing_firstname"],
                            lastName     : order["billing_lastname"],
                            mobileNumber : order["billing_telephone"],
                            city         : order["billing_city"],
                            pinCode      : order["billing_postcode"],
                            country      : CommonService.countryMap[order["billing_country_id"]],
                            state        : order["billing_region"],
                            addressLine1 : billingAddressLine1,
                            addressLine2 : billingAddressLine2,
                            isDefault    : false,
                            isDeleted    : false,
                            dateCreated  : order["created_at"] ? order["created_at"].time : timeStamp,
                            lastUpdatedBy: '',
                            lastUpdated  : order["created_at"] ? order["created_at"].time : timeStamp,
                            type         : "billing",
                    ]

                    /*Capturing Shipping Address Info*/
                    List shippingAddressLines = order["shipping_street"].toString().tokenize("\n")
                    def shippingAddressLine1 = billingAddressLines[0]
                    def shippingAddressLine2 = null
                    if (shippingAddressLines.size() > 1) {
                        shippingAddressLines.remove(0)
                        shippingAddressLine2 = shippingAddressLines.join("\n")
                    }

                    Map shippingAddress = [
                            id           : order["shipping_entity_id"].toString(),
                            nickName     : '',
                            title        : order["shipping_prefix"],
                            firstName    : order["shipping_firstname"],
                            lastName     : order["shipping_lastname"],
                            mobileNumber : order["shipping_telephone"],
                            city         : order["shipping_city"],
                            pinCode      : order["shipping_postcode"],
                            country      : CommonService.countryMap[order["shipping_country_id"]],
                            state        : order["shipping_region"],
                            addressLine1 : shippingAddressLine1,
                            addressLine2 : shippingAddressLine2,
                            isDefault    : false,
                            isDeleted    : false,
                            dateCreated  : order["created_at"] ? order["created_at"].time : timeStamp,
                            lastUpdatedBy: '',
                            lastUpdated  : order["created_at"] ? order["created_at"].time : timeStamp,
                            type         : "delivery",
                    ]

                    /*Capturing User Info*/
                    Map userInfo = [
                            emailId      : order["customer_email"],
                            title        : order["customer_prefix"],
                            firstName    : order["customer_firstname"],
                            lastName     : order["customer_lastname"],
                            mobileNumber : order["customer_telephone"], //which phone number
                            gender       : CommonService.customerGenderMap[order["customer_gender"].toString()],
                            zipCode      : null, //which postcode
                            country      : null, //which country
                            state        : null, //which region
                            city         : null, //which city
                            dateOfBirth  : order["customer_dob"] ? order["customer_dob"].time : null,
                            maritalStatus: null,
                    ]

                    /*Capturing UTM Data*/
                    Map utmData = [
                            source  : order["source"],
                            campaign: order["campaign"],
                            medium  : null,
                            term    : null,
                            content : order["utm_text"],
                            page_url: null,
                    ]

                    Float storeCredits = order["customer_balance_amount"] ? order["customer_balance_amount"] : 0

                    /*Capturing Tender Info*/
                    List tenderInfoList = []
                    if (storeCredits > 0) {
                        Map tenderInfo = [
                                paymentProvider: "STORE_CREDIT",
                                amount         : storeCredits,
                        ]
                        tenderInfoList << tenderInfo
                    }
                    if (order["grand_total"] > 0) {
                        Map tenderInfo = [
                                paymentProvider: CommonService.paymentProviderMap[order["method"].toString()],
                                amount         : order["grand_total"],
                        ]
                        tenderInfoList << tenderInfo
                    }

                    Map orderMap = [
                            id                     : order["entity_id"].toString(),
                            orderId                : order["increment_id"],
                            userId                 : order["customer_id"].toString(),
                            messageTrackingId      : null,
                            orderStatus            : order["status"] ? CommonService.orderStatusMap[order["status"]] : 'CLOSED',
                            orderReturnStatus      : orderReturnStatus,
                            paymentType            : CommonService.paymentTypeMap[order["method"]],
                            paymentProvider        : CommonService.paymentProviderMap[order["method"]],
                            isStoreCreditUsed      : storeCredits > 0 ? true : false,
                            isOrderCancelledByAdmin: false,
                            dcStatus               : CommonService.orderDCStatusMap[order["sent_to_erp"].toString()],
                            deliveryStatus         : null,
                            currency               : order["order_currency_code"],
                            taxAmount              : order["subtotal_incl_tax"] - order["subtotal"],
                            //taxPercentage          : order["percent"],
                            //taxCategory            : order["code"],
                            subtotal               : order["subtotal_incl_tax"],
                            grandTotal             : order["grand_total"] + storeCredits,
                            totalPaid              : order["total_paid"] ?: 0,
                            shippingCharges        : order["shipping_incl_tax"],
                            codCharges             : 0,
                            purchasedFrom          : 'American Swan,American Swan,American Swan',
                            exchangeRate           : 1,
                            ipAddress              : order["remote_ip"],
                            isCreatedByAdmin       : false,
                            isDeleted              : false,
                            dateCreated            : order["created_at"] ? order["created_at"].time : timeStamp,
                            lastUpdatedBy          : '',
                            lastUpdated            : order["updated_at"] ? order["updated_at"].time : timeStamp,
                            orderStatusLogList     : orderStatusLogList,
                            couponDetail           : couponDetail,
                            billingAddress         : billingAddress,
                            shippingAddress        : shippingAddress,
                            orderItems             : orderItemList,
                            userInfo               : userInfo,
                            paymentDetail          : null, //pending
                            payTmDetail            : null, //pending
                            tenderInfoList         : tenderInfoList, //Discussion done with Tushar, details of pending payment are also stored in this. No status maintained.
                            utmData                : utmData,
                            expectedDeliveryDays   : null, //not maintained by magento
                    ]

                    orderList << orderMap

                    List invoices = getInvoicesFromDb(order["entity_id"], hSessionInner)
                    invoices.each { invoice ->

                        List invoiceItems = getInvoiceItemsFromDb(invoice["entity_id"], hSessionInner)


                        def totalMRPValue = 0
                        def totalProductDiscountAmount = 0
                        def totalQuantity = 0

                        List<Map> invoiceItemList = []
                        invoiceItems.each { invoiceItem ->

                            Map orderItemMap = orderItemList.find {
                                it["id"].equals(invoiceItem["order_item_id"].toString())
                            }

                            if (orderItemMap) {

                                Map productInfo = orderItemMap["productInfo"]

                                def taxCSTPercent = 0
                                def taxCSTAmount = 0
                                def taxVATPercent = 0
                                def taxVATAmount = 0

                                if (taxCategory.equals("VAT")) {
                                    taxVATPercent = orderItemMap["taxPercentage"]
                                    taxVATAmount = (invoiceItem["row_total_incl_tax"] ? invoiceItem["row_total_incl_tax"] : 0.0) - (invoiceItem["row_total"] ? invoiceItem["row_total"] : 0.0)
                                } else if (taxCategory.equals("CST")) {
                                    taxCSTPercent = orderItemMap["taxPercentage"]
                                    taxCSTAmount = (invoiceItem["row_total_incl_tax"] ? invoiceItem["row_total_incl_tax"] : 0.0) - (invoiceItem["row_total"] ? invoiceItem["row_total"] : 0.0)
                                }

                                totalMRPValue += orderItemMap["price"] * invoiceItem["qty"]

                                def productDiscountAmount = (orderItemMap["price"] - (invoiceItem["price_incl_tax"] ? invoiceItem["price_incl_tax"] : 0.0)) * invoiceItem["qty"]
                                totalProductDiscountAmount += productDiscountAmount

                                totalQuantity += invoiceItem["qty"] as int

                                Map invoiceItemMap = [
                                        id                   : invoiceItem["entity_id"].toString(),
                                        serialNo             : null,
                                        eanCode              : productInfo["eanNumber"],
                                        sku                  : productInfo["sku"],
                                        name                 : orderItemMap["productName"],
                                        options              : null,
                                        mrp                  : orderItemMap["price"],
                                        productDiscountAmount: productDiscountAmount,
                                        qtyInvoiced          : invoiceItem["qty"] ? invoiceItem["qty"] as int : 0,
                                        subTotal             : invoiceItem["row_total_incl_tax"],
                                        couponDiscountAmount : invoiceItem["discount_amount"] ? invoiceItem["discount_amount"]?.abs() : 0.0,
                                        taxCSTPercent        : taxCSTPercent,
                                        taxCSTAmount         : taxCSTAmount,
                                        taxVATPercent        : taxVATPercent,
                                        taxVATAmount         : taxVATAmount,
                                        rowTotal             : invoiceItem["row_total_incl_tax"], //To be Commented after discussion with Tushar as it is not in use.
                                ]
                                invoiceItemList << invoiceItemMap
                            }
                        }

                        Map invoiceMap = [
                                id                        : invoice["entity_id"].toString(),
                                invoiceNumber             : invoice["increment_id"],
                                invoicedDate              : invoice["created_at"] ? invoice["created_at"].time : timeStamp,
                                orderNumber               : order["increment_id"],
                                paymentMethod             : null,
                                totalQty                  : totalQuantity,
                                totalMRPValue             : totalMRPValue,
                                totalProductDiscountAmount: totalProductDiscountAmount,
                                totalCouponDiscountAmount : invoice["discount_amount"] ? invoice["discount_amount"]?.abs() : 0.0,
                                totalTaxableAmount        : invoice["subtotal"],
                                totalTaxCSTAmount         : taxCategory == "CST" ? invoice["tax_amount"] : 0,
                                totalTaxVATAmount         : taxCategory == "VAT" ? invoice["tax_amount"] : 0,
                                rowsSubTotal              : invoice["subtotal_incl_tax"],
                                shippingCharges           : invoice["shipping_incl_tax"],
                                codCharge                 : 0,
                                netInvoiceValue           : invoice["grand_total"],
                                roundingOffNetValue       : Math.round(invoice["grand_total"]),
                                amountInWords             : null,
                                currency                  : invoice["order_currency_code"],
                                paymentProvider           : orderMap["paymentProvider"],
                                isDeleted                 : false,
                                dateCreated               : invoice["created_at"] ? invoice["created_at"].time : timeStamp,
                                lastUpdatedBy             : '',
                                lastUpdated               : invoice["updated_at"] ? invoice["updated_at"].time : timeStamp,
                                vendorCompanyAddress      : null,
                                invoiceItemList           : invoiceItemList,
                        ]

                        invoiceList << invoiceMap
                    }

                    List rmas = getRMAsFromDb(order["entity_id"], hSessionInner)
                    Map rmaItemListMap = [:]
                    rmas.each { rma ->

                        List rmaItems = getRMAItemsFromDb(rma["entity_id"], hSessionInner)

                        List<Map> rmaItemList = []
                        rmaItems.each { rmaItem ->

                            Map orderItemMap = orderItemList.find {
                                it["id"].equals(rmaItem["order_item_id"].toString())
                            }
                            if (orderItemMap) {
                                Map rmaItemMap = orderItemMap.clone()
                                rmaItemMap["id"] = rmaItem["entity_id"].toString()
                                List rmaItemAttributes = getRMAItemAttributes(rmaItem["entity_id"], hSessionInner)
                                rmaItemMap = commonService.addAttributes(rmaItemMap, rmaItemAttributes)
                                rmaItemMap << [qtyReturned: (rmaItem["qty_requested"] ? rmaItem["qty_requested"] as int : 0)]

                                rmaItemListMap[rmaItem["order_item_id"].toString()] = rmaItemMap

                                rmaItemList << rmaItemMap
                            }
                        }

                        List rmaComments = getRMACommentsFromDb(rma["entity_id"], hSessionInner)
                        List<Map> returnCommentHistoryList = []
                        rmaComments.each { rmaComment ->

                            Map returnCommentMap = [
                                    id           : rmaComment["entity_id"].toString(),
                                    message      : rmaComment["comment"],
                                    isDeleted    : false,
                                    dateCreated  : rmaComment["created_at"] ? rmaComment["created_at"].time : timeStamp,
                                    lastUpdatedBy: '',
                                    lastUpdated  : rmaComment["created_at"] ? rmaComment["created_at"].time : timeStamp,
                            ]

                            returnCommentHistoryList << returnCommentMap
                        }

                        Map rmaMap = [
                                id                      : rma["entity_id"].toString(),
                                rmaStatus               : commonService.rmaStatusMap[rma["status"]],
                                rmaNumber               : rma["increment_id"],
                                orderId                 : order["entity_id"].toString(),
                                createdBy               : '',
                                isDeleted               : false,
                                dateCreated             : rma["date_requested"] ? rma["date_requested"].time : timeStamp,
                                lastUpdatedBy           : '',
                                lastUpdated             : rma["date_requested"] ? rma["date_requested"].time : timeStamp,
                                returnCommentHistoryList: returnCommentHistoryList,
                                returnItems             : rmaItemList,
                                rmaType                 : "RL"
                        ]

                        rmaList << rmaMap
                    }


                    List creditMemos = getCreditMemosFromDb(order["entity_id"], hSessionInner)
                    creditMemos.each { creditMemo ->

                        List creditMemoItems = getCreditMemoItemsFromDb(creditMemo["entity_id"], hSessionInner)

                        List<Map> creditMemoItemList = []
                        creditMemoItems.each { creditMemoItem ->

                            Map orderItemMap = rmaItemListMap[creditMemoItem["order_item_id"].toString()]
                            if (!orderItemMap) {
                                orderItemMap = orderItemList.find {
                                    it["id"].equals(creditMemoItem["order_item_id"].toString())
                                }
                            }
                            if (orderItemMap) {
                                Map creditMemoItemMap = orderItemMap.clone()
                                creditMemoItemMap["id"] = creditMemoItem["entity_id"].toString()
                                creditMemoItemMap << [qtyReturned: (creditMemoItem["qty"] ? creditMemoItem["qty"] as int : 0)]

                                creditMemoItemList << creditMemoItemMap
                            }

                        }

                        //def creditsUsed = creditMemo["customer_balance_amount"] ? creditMemo["customer_balance_amount"] : 0
                        def creditRefundAmount = creditMemo["customer_bal_total_refunded"] > 0 ? creditMemo["customer_bal_total_refunded"] : 0

                        Map creditMemoMap = [
                                id                   : creditMemo["entity_id"].toString(),
                                status               : commonService.creditMemoStatusMap[creditMemo["state"].toString()],
                                creditMemoNumber     : creditMemo["increment_id"],
                                orderId              : order["entity_id"].toString(),
                                orderNumber          : order["increment_id"],
                                isCustomerNotified   : creditMemo["email_sent"] ? true : false,
                                isRefundToStoreCredit: creditRefundAmount > 0,
                                refundToStoreCredit  : creditRefundAmount as float,
                                totalRefund          : creditRefundAmount,
                                isDeleted            : false,
                                dateCreated          : creditMemo["created_at"] ? creditMemo["created_at"].time : timeStamp,
                                lastUpdatedBy        : '',
                                lastUpdated          : creditMemo["updated_at"] ? creditMemo["updated_at"].time : timeStamp,
                                returnItems          : creditMemoItemList,
                        ]

                        creditMemoList << creditMemoMap
                    }


                    List shipments = getShipmentsFromDb(order["entity_id"], hSessionInner)
                    shipments.each { shipment ->

                        Map shipmentMap = [
                                id               : shipment["entity_id"].toString(),
                                shipmentNumber   : shipment["increment_id"],
                                orderNumber      : order["increment_id"],
                                shipmentDate     : shipment["created_at"] ? shipment["created_at"].time : timeStamp,
                                trackingTitle    : shipment["title"],
                                trackNumber      : shipment["track_number"],
                                status           : "SHIPPED",
                                comments         : shipment["comment"],
                                isDeleted        : false,
                                dateCreated      : shipment["created_at"] ? shipment["created_at"].time : timeStamp,
                                lastUpdatedBy    : '',
                                lastUpdated      : shipment["updated_at"] ? shipment["updated_at"].time : timeStamp,
                                messageTrackingId: null,
                        ]

                        shipmentList << shipmentMap
                    }

                    Map errorMap = validateOrder(orderMap) //Do validation here
                    if (errorMap) {
                        synchronized (failedLock) {
                            failed++
                        }
                        /*SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }*/

                        try {
                            SQLQuery query = hSessionInner.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
                            VALUES (:entity_id,:failure_reason,:progress_index_id,0)""")
                            query.with {
                                setLong("entity_id", errorMap["entityId"] as long)
                                setLong("progress_index_id", progressIndexId)
                                setString("failure_reason", errorMap["failureReason"])
                                executeUpdate()
                            }
                        }
                        catch (ConstraintViolationException e) {
                            //log.error e.localizedMessage
                        }


                    } else {
                        synchronized (succeededLock) {
                            succeeded++
                        }
                        /*SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_valid=count_valid+1,count_iteration_valid=count_iteration_valid+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }*/
                    }
                    synchronized (processedLock) {
                        processed++
                    }

                }
                catch (Exception e) {
                    e.printStackTrace()
                    errorTrackingMessage = "error"
                    log.error e.localizedMessage
                }
                finally {
                    if (hSessionInner) {
                        hSessionInner.flush()
                        hSessionInner.close()
                    }
                }
            }

            ExecutorService pool = Executors.newFixedThreadPool(MigrationConstants.THREAD_POOL_SIZE);
            orders.each { order ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ orderCallable(order, hSessionInner) } as Callable)
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)

            SQLQuery query2 = hSession.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+:processed, count_valid=count_valid+:succeeded,count_iteration_valid=:succeeded1, count_fail=count_fail+:failed,count_iteration_fail=:failed1 WHERE id =:id""")
            query2.with {
                setLong("id", progressIndexId)
                setInteger("processed", processed)
                setInteger("succeeded", succeeded)
                setInteger("failed", failed)
                setInteger("succeeded1", succeeded)
                setInteger("failed1", failed)
                executeUpdate()
            }

            if (errorTrackingMessage) {
                throw new MigrationException("Worker threads are unable to process due to some reason")
            }
            Date date2 = new Date()
            log.info("Time to process " + orders.size() + " orders : " + (date2.time - date1.time) / 1000)

            if (orderCouponList) {
                elasticSearchService.migrate(orderCouponList, "order-coupon", "order-coupon", progressId, hSession, progressIndexId, false)
            }
            if (shipmentList) {
                elasticSearchService.migrate(shipmentList, "shipment", "shipment", progressId, hSession, progressIndexId, false)
            }
            if (invoiceList) {
                elasticSearchService.migrate(invoiceList, "invoice", "invoice", progressId, hSession, progressIndexId, false)
            }
            if (creditMemoList) {
                elasticSearchService.migrate(creditMemoList, "credit_memo", "credit_memo", progressId, hSession, progressIndexId, false)
            }
            if (rmaList) {
                elasticSearchService.migrate(rmaList, "return_merchandise_authorization", "return_merchandise_authorization", progressId, hSession, progressIndexId, false)
            }
            elasticSearchService.migrate(orderList, "order", "order", progressId, hSession, progressIndexId)
            Date date4 = new Date()
            log.info("Time to index " + orders.size() + " orders : " + (date4.time - date2.time) / 1000)
            log.info("Starting next iteration")
            Long resumePoint = counter + orders.size()
            counter += MigrationConstants.ORDER_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            Date date5 = new Date()
            log.info("Total Time to complete " + orders.size() + " orders migration : " + (date5.time - date1.time) / 1000)
            populateOrder(progressId, counter, orderCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getOrdersFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT sfo.entity_id,sfo.increment_id,sfo.customer_id,sfo.status,sfop.method,sfo.sent_to_erp,sfo.order_currency_code,sfo.tax_amount,
        sfo.subtotal_incl_tax,sfo.subtotal,sfo.grand_total,sfo.total_paid,sfo.shipping_incl_tax,sfo.remote_ip,sfo.created_at,sfo.updated_at,
        sfo.customer_email,sfo.customer_firstname,sfo.customer_lastname,sfo.customer_gender,sfo.customer_dob,sfo.customer_prefix,ecsfo.customer_telephone,
        sfo.applied_rule_ids,sfo.coupon_code,sfo.discount_amount,
        sfo.customer_balance_amount,sfo.customer_balance_refunded,sfo.customer_bal_total_refunded,
        sfo.source,sfo.campaign,sfo.utm_text,
        sfoab.entity_id AS billing_entity_id,
        sfoab.prefix AS billing_prefix, sfoab.firstname AS billing_firstname,sfoab.lastname AS billing_lastname,sfoab.middlename AS billing_middlename,sfoab.telephone AS billing_telephone,
        sfoab.city AS billing_city,sfoab.postcode AS billing_postcode,sfoab.country_id AS billing_country_id,sfoab.region AS billing_region,sfoab.street AS billing_street,
        sfoas.entity_id AS shipping_entity_id,sfoas.prefix AS shipping_prefix, sfoas.firstname AS shipping_firstname,sfoas.lastname AS shipping_lastname,sfoas.middlename AS shipping_middlename,
        sfoas.telephone AS shipping_telephone,sfoas.city AS shipping_city,sfoas.postcode AS shipping_postcode,sfoas.country_id AS shipping_country_id,sfoas.region AS shipping_region,sfoas.street AS shipping_street
        FROM sales_flat_order sfo LEFT JOIN sales_flat_order_payment sfop ON sfo.entity_id=sfop.parent_id LEFT JOIN enterprise_customer_sales_flat_order ecsfo ON sfo.entity_id = ecsfo.entity_id
        LEFT JOIN sales_flat_order_address sfoab ON sfo.entity_id=sfoab.parent_id AND sfoab.address_type='billing'
        LEFT JOIN sales_flat_order_address sfoas ON sfo.entity_id=sfoas.parent_id AND sfoas.address_type='shipping' LIMIT :limit,:totalSize""")
        def orders = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.ORDER_ITERATION_SIZE)
            list()
        }
        orders
    }

    def getOrderStatusLogsFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,is_customer_notified,comment,status,created_at FROM sales_flat_order_status_history WHERE parent_id = :orderId""")
        def orderStatusLogs = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        orderStatusLogs
    }

    def getOrderTaxCategoryFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""select code from sales_order_tax where order_id = :orderId group by order_id""")
        def taxCategory = query.with {
            setLong("orderId", orderId)
            uniqueResult()
        }
        taxCategory
    }

    def getOrderItemsFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT parent.item_id,parent.product_id,child.product_id as product_var_id,parent.name,parent.sku,parent.qty_ordered,
        parent.qty_canceled,parent.qty_invoiced,parent.qty_shipped,parent.qty_refunded,parent.qty_returned,
        parent.product_mrp,parent.original_price,parent.price,parent.tax_amount,parent.hidden_tax_amount,sot.code,parent.tax_percent,parent.discount_amount,parent.row_total_incl_tax,parent.row_total
        FROM sales_flat_order_item parent LEFT JOIN sales_flat_order_item child ON parent.item_id=child.parent_item_id LEFT JOIN
        sales_order_tax_item soti ON parent.item_id = soti.item_id LEFT JOIN sales_order_tax sot ON sot.tax_id = soti.tax_id
        WHERE parent.order_id=:orderId AND parent.parent_item_id IS NULL;""")
        def orderItems = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        orderItems
    }

    List getProductAttributes(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT attribute_id, value FROM catalog_product_entity_datetime
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                UNION
                SELECT attribute_id, value FROM catalog_product_entity_varchar
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                UNION
                SELECT attribute_id, value FROM catalog_product_entity_int
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                UNION
                SELECT attribute_id, value FROM catalog_product_entity_decimal
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                UNION
                SELECT attribute_id, value FROM catalog_product_entity_text
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id""")
        def attributes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("id", id)
            list()
        }
        attributes
    }

    def getInvoicesFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT sfi.entity_id,sfi.increment_id,sfi.total_qty,sfi.subtotal_incl_tax,sfi.discount_amount,sfi.subtotal,
        sfi.tax_amount,sfi.shipping_incl_tax,sfi.grand_total,sfi.order_currency_code,sfi.created_at,sfi.updated_at FROM sales_flat_invoice sfi WHERE order_id = :orderId""")
        def invoices = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        invoices
    }

    def getInvoiceItemsFromDb(Integer invoiceId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,discount_amount,row_total,row_total_incl_tax,price_incl_tax,qty,order_item_id FROM sales_flat_invoice_item WHERE parent_id = :invoiceId""")
        def invoiceItems = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("invoiceId", invoiceId)
            list()
        }
        invoiceItems
    }

    def getCreditMemosFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,increment_id,email_sent,customer_bal_total_refunded,customer_balance_amount,grand_total,created_at,updated_at,state FROM sales_flat_creditmemo WHERE order_id = :orderId""")
        def creditMemos = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        creditMemos
    }

    def getCreditMemoItemsFromDb(Integer creditMemoId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,order_item_id,qty FROM sales_flat_creditmemo_item WHERE parent_id = :creditMemoId""")
        def creditMemoItems = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("creditMemoId", creditMemoId)
            list()
        }
        creditMemoItems
    }

    def getRMAsFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,status,increment_id,date_requested FROM enterprise_rma WHERE order_id = :orderId""")
        def rmas = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        rmas
    }

    def getRMAItemsFromDb(Integer rmaId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,qty_requested,qty_approved,qty_authorized,qty_returned,status,order_item_id FROM enterprise_rma_item_entity WHERE rma_entity_id = :rmaId""")
        def rmaItems = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rmaId", rmaId)
            list()
        }
        rmaItems
    }

    def getRMACommentsFromDb(Integer rmaId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT entity_id,comment,status,created_at FROM enterprise_rma_status_history WHERE rma_entity_id = :rmaId""")
        def rmaComments = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rmaId", rmaId)
            list()
        }
        rmaComments
    }

    def getShipmentsFromDb(Integer orderId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT sfs.entity_id,sfs.increment_id,sfst.title,sfst.track_number,sfs.shipment_status,sfsc.comment,sfs.created_at,sfs.updated_at
        FROM sales_flat_shipment sfs LEFT JOIN sales_flat_shipment_track sfst ON sfs.entity_id = sfst.parent_id LEFT JOIN
        sales_flat_shipment_comment sfsc ON sfs.entity_id = sfsc.parent_id WHERE sfs.order_id = :orderId""")
        def shipments = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("orderId", orderId)
            list()
        }
        shipments
    }

    List getRMAItemAttributes(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT attribute_id, value FROM enterprise_rma_item_entity_int
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                UNION
                SELECT attribute_id, value FROM enterprise_rma_item_entity_varchar
                WHERE value IS NOT NULL AND entity_id =:id GROUP BY attribute_id
                """)
        def attributes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("id", id)
            list()
        }
        attributes
    }

    def getProductCategoryInfoFromDb(Long productId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT ccp.category_id ,cce.path
        FROM catalog_category_product ccp
        LEFT JOIN catalog_category_entity cce ON cce.entity_id = ccp.category_id
        WHERE ccp.product_id = :product_id AND cce.children_count=0 and ccp.category_id NOT IN (344,345,178,521,501,349,348,179,531,511) ORDER BY ccp.category_id LIMIT 0,1""")
        def productCategoryInfo = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("product_id", productId)
            uniqueResult()
        }
        productCategoryInfo
    }

    def getCategoryNamesFromDb(List categoryIds, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT ccev.entity_id, ccev.value
        FROM catalog_category_entity_varchar ccev WHERE ccev.entity_id IN (:entity_ids) AND ccev.attribute_id = 41""")
        def categoryNames = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setParameterList("entity_ids", categoryIds)
            list()
        }
        categoryNames
    }

    def getProductSkuFromDb(Long productId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT sku FROM catalog_product_entity WHERE entity_id = :productId")
        def sku = query.with {
            setLong("productId", productId)
            uniqueResult()
        }
        sku
    }

    def getCouponFromDb(Long ruleId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT sr.rule_id, sr.name,sr.description,sr.from_date,sr.to_date,sr.is_active,sr.sort_order,sr.coupon_type,
        sr.voucher_type,srl.label, sr.uses_per_coupon,sr.uses_per_customer,sr.times_used,sr.discount_amount,sr.discount_qty,sr.simple_free_shipping,sr.simple_action,
        sr.use_auto_generation FROM salesrule sr LEFT JOIN salesrule_label srl ON sr.rule_id = srl.rule_id WHERE sr.rule_id =:rule_id""")
        def coupon = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rule_id", ruleId)
            uniqueResult()
        }
        coupon
    }

    Map validateOrder(Map order) {
        Map errorMap = null

        OrderCO orderCO = new OrderCO()
        DataBindingUtils.bindObjectToInstance(orderCO, order, [], ["orderStatusLogList", "couponDetail", "billingAddress", "shippingAddress", "orderItems", "userInfo", "tenderInfoList", "errorCodes"], null)

        List<OrderStatusCO> orderStatusLogList = []
        order.orderStatusLogList.each {
            OrderStatusCO orderStatusCO = new OrderStatusCO()
            DataBindingUtils.bindObjectToInstance(orderStatusCO, it, ["id", "orderStatus", "isCustomerNotified"], [], null)
            orderStatusLogList << orderStatusCO
        }
        orderCO.orderStatusLogList = orderStatusLogList

        if (order.couponDetail) {
            CouponDetailCO couponDetailCO = new CouponDetailCO()
            DataBindingUtils.bindObjectToInstance(couponDetailCO, order.couponDetail, ["couponId", "couponCode", "couponValue"], [], null)
            orderCO.couponDetail = couponDetailCO
        }

        AddressCO billingAddressCO = new AddressCO()
        DataBindingUtils.bindObjectToInstance(billingAddressCO, order.billingAddress, ["id", "firstName", "mobileNumber", "city", "pinCode", "country", "state", "addressLine1"], [], null)
        orderCO.billingAddress = billingAddressCO

        AddressCO shippingAddressCO = new AddressCO()
        DataBindingUtils.bindObjectToInstance(shippingAddressCO, order.shippingAddress, ["id", "firstName", "mobileNumber", "city", "pinCode", "country", "state", "addressLine1"], [], null)
        orderCO.shippingAddress = shippingAddressCO

        List<OrderItemCO> orderItems = []
        order.orderItems.each {
            OrderItemCO orderItemCO = new OrderItemCO()
            DataBindingUtils.bindObjectToInstance(orderItemCO, it, [], ["productInfo", "errorCodes"], null)
            ProductInfoCO productInfo = new ProductInfoCO()
            DataBindingUtils.bindObjectToInstance(productInfo, it.productInfo, [], [], null)
            orderItemCO.productInfo = productInfo
            orderItems << orderItemCO
        }
        orderCO.orderItems = orderItems

        UserInfoCO userInfo = new UserInfoCO()
        DataBindingUtils.bindObjectToInstance(userInfo, order.userInfo, [], [], null)
        orderCO.userInfo = userInfo

        List<TenderInfoCO> tenderInfoList = []
        order.tenderInfoList.each {
            TenderInfoCO tenderInfoCO = new TenderInfoCO()
            DataBindingUtils.bindObjectToInstance(tenderInfoCO, it, [], [], null)
            tenderInfoList << tenderInfoCO
        }
        orderCO.tenderInfoList = tenderInfoList


        if (!orderCO.validate() || orderCO.errorCodes.size() > 0) {
            StringBuilder builder = new StringBuilder()
            builder.append(orderCO.errors.allErrors.collect {
                messageSource.getMessage(it, Locale.default)
            }.join("<br/>"))
            builder.append("<br/>")
            builder.append(orderCO.errorCodes.collect {
                messageSource.getMessage(it, Locale.default)
            }.join("<br/>"))
            String message = new String(builder.toString())
            log.error "Validation failed for order : [${orderCO.id} | ${orderCO.orderId}], message : ${message}"
            errorMap = [
                    entityId     : orderCO.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }
}
