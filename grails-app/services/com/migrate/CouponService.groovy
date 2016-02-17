package com.migrate

import com.migrate.co.CouponCO
import com.migrate.enums.ProgressStatus
import com.migrate.exception.MigrationException
import groovy.transform.TailRecursive
import migration.Progress
import migration.ProgressIndex
import org.hibernate.SQLQuery
import org.hibernate.Session
import org.hibernate.SessionFactory
import org.hibernate.exception.ConstraintViolationException
import org.hibernate.transform.AliasToEntityMapResultTransformer

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CouponService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    def messageSource

    static Map actionConditionMap = [
            "by_percent"        : "PERCENT_OF_PRODUCT_PRICE_DISCOUNT",
            "by_fixed"          : "FIXED_AMOUNT_DISCOUNT",
            "cart_fixed"        : "FIXED_AMOUNT_DISCOUNT_FOR_WHOLE_CART",
            "the_cheapest"      : "PERCENT_DISCOUNT_FOR_THE_CHEAPEST",
            "the_most_expencive": "PERCENT_DISCOUNT_FOR_THE_MOST_EXPENSIVE"
    ]

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Coupon migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long couponCount = hSession.createSQLQuery("SELECT count(sr.rule_id) FROM salesrule sr").list().first() as Long
            // WHERE sr.times_used>0
            log.info(couponCount + " many coupons are found to migrate")
            Long timeStamp = new Date().time

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='COUPON'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < couponCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + couponCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", couponCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Coupons")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "COUPON")
                    setLong("total_documents", couponCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='COUPON'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            if (counter < couponCount) {
                populateCoupon(progressId, counter, couponCount, timeStamp, hSession, progressIndexId)

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

            log.info("-----------------------------Ending Coupon migration--------------------------------------------")
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
    Boolean populateCoupon(Long progressId, Long counter, Long couponCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping coupon migration-------------------------------------------")
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

        if (counter < couponCount) {

            List coupons = getCouponsFromDb(counter, hSession)

            log.info("Fetched " + coupons.size() + " many coupons from db to migrate in one iteration")
            List<Map> couponList = [].asSynchronized()
            List<Map> couponCodeList = [].asSynchronized()
            List<Map> couponUserList = [].asSynchronized()

            String errorTrackingMessage = null

            def couponCallable = { coupon, hSessionInner ->

                try {
                    boolean couponHasCode = false
                    List emailIds = []
                    def couponType = null
                    if (coupon["coupon_type"] == 1) {
                        couponType = 'NO_COUPON'
                    } else if (coupon["coupon_type"] == 2 && coupon["use_auto_generation"] == 0) {
                        couponType = 'SPECIFIC_COUPON'
                    } else {
                        couponType = 'AUTO'
                    }

                    if (couponType == 'SPECIFIC_COUPON') {

                        Map couponCodeDBMap = getCouponCodeFromDb(coupon["rule_id"], hSessionInner)
                        if (couponCodeDBMap) {
                            couponHasCode = true
                            Map couponCodeMap = [
                                    id           : couponCodeDBMap["coupon_id"].toString(),
                                    couponId     : coupon["rule_id"].toString(),
                                    couponCode   : couponCodeDBMap["code"].toString(),
                                    couponType   : couponType,
                                    dateCreated  : couponCodeDBMap["created_at"] ? couponCodeDBMap["created_at"].time : timeStamp,
                                    lastUpdated  : couponCodeDBMap["created_at"] ? couponCodeDBMap["created_at"].time : timeStamp,
                                    isDeleted    : false,
                                    lastUpdatedBy: null,
                            ]

                            couponCodeList << couponCodeMap

                            if (couponCodeDBMap["customer_id"]) {

                                Map couponUserMap = [
                                        id           : couponCodeDBMap["coupon_id"].toString(),
                                        couponId     : coupon["rule_id"].toString(),
                                        couponCode   : couponCodeDBMap["code"].toString(),
                                        couponType   : couponType,
                                        userId       : couponCodeDBMap["customer_id"].toString(),
                                        dateCreated  : couponCodeDBMap["created_at"] ? couponCodeDBMap["created_at"].time : timeStamp,
                                        lastUpdated  : couponCodeDBMap["created_at"] ? couponCodeDBMap["created_at"].time : timeStamp,
                                        isDeleted    : false,
                                        lastUpdatedBy: null,
                                ]
                                couponUserList << couponUserMap

                                emailIds << couponCodeDBMap["customer_email"]
                            }
                        }

                    } else if (couponType == 'AUTO') {

/*                        Long couponCodeCount = coupon["couponCodeCount"]
                        Long couponCodeCounter = 0
                        while(couponCodeCounter < couponCodeCount){
                            log.info("Migrating "+MigrationConstants.COUPON_CODE_ITERATION_SIZE+" coupon codes for rule "+coupon["rule_id"])
                            List autoGenCoupons = getAutoGenCouponCodesFromDb(coupon["rule_id"], hSessionInner, couponCodeCounter)

                            List<Map> localCouponCodeList = []
                            List<Map> localCouponUserList = []
                            autoGenCoupons.each { autoGenCoupon ->
                                couponHasCode = true

                                Map couponCodeMap = [
                                        id           : autoGenCoupon["coupon_id"].toString(),
                                        couponId     : coupon["rule_id"].toString(),
                                        couponCode   : autoGenCoupon["code"].toString(),
                                        couponType   : couponType,
                                        dateCreated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                        lastUpdated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                        isDeleted    : false,
                                        lastUpdatedBy: null,
                                ]

                                localCouponCodeList << couponCodeMap

                                if (autoGenCoupon["customer_id"]) {

                                    Map couponUserMap = [
                                            id           : autoGenCoupon["coupon_id"].toString(),
                                            couponId     : coupon["rule_id"].toString(),
                                            couponCode   : autoGenCoupon["code"].toString(),
                                            couponType   : couponType,
                                            userId       : autoGenCoupon["customer_id"].toString(),
                                            dateCreated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                            lastUpdated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                            isDeleted    : false,
                                            lastUpdatedBy: null,
                                    ]
                                    localCouponUserList << couponUserMap

                                }
                            }

                            if (localCouponUserList) {
                                elasticSearchService.migrate(localCouponUserList, "coupon-user", "coupon-user", progressId, hSession, progressIndexId, false)
                            }
                            if (localCouponCodeList) {
                                elasticSearchService.migrate(localCouponCodeList, "coupon-code", "coupon-code", progressId, hSession, progressIndexId, false)
                            }

                            couponCodeCounter +=MigrationConstants.COUPON_CODE_ITERATION_SIZE
                        }*/

                        List autoGenCoupons = getAutoGenCouponCodesFromDb(coupon["rule_id"], hSessionInner)

                        autoGenCoupons.each { autoGenCoupon ->
                            couponHasCode = true

                            Map couponCodeMap = [
                                    id           : autoGenCoupon["coupon_id"].toString(),
                                    couponId     : coupon["rule_id"].toString(),
                                    couponCode   : autoGenCoupon["code"].toString(),
                                    couponType   : couponType,
                                    dateCreated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                    lastUpdated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                    isDeleted    : false,
                                    lastUpdatedBy: null,
                            ]

                            couponCodeList << couponCodeMap

                            if (autoGenCoupon["customer_id"]) {

                                Map couponUserMap = [
                                        id           : autoGenCoupon["coupon_id"].toString(),
                                        couponId     : coupon["rule_id"].toString(),
                                        couponCode   : autoGenCoupon["code"].toString(),
                                        couponType   : couponType,
                                        userId       : autoGenCoupon["customer_id"].toString(),
                                        dateCreated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                        lastUpdated  : autoGenCoupon["created_at"] ? autoGenCoupon["created_at"].time : timeStamp,
                                        isDeleted    : false,
                                        lastUpdatedBy: null,
                                ]
                                couponUserList << couponUserMap

                            }
                        }

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

                    Map couponRuleAction = [
                            couponActionType   : actionConditionMap[coupon["simple_action"]],
                            value              : coupon["discount_amount"] as Double,
                            discountIsAppliedTo: coupon["discount_qty"] as Integer,
                            freeShipping       : coupon["simple_free_shipping"] == 1 ? 'YES' : 'NO',
                            lineItemFilterNode : null,
                            useProductMRP      : false,
                    ]

                    Map couponCodedetail = [
                            quantity             : 0,
                            length               : 0,
                            dashCharacterInterval: 0,
                            prefix               : "",
                            suffix               : "",
                            couponCodeFormat     : null,
                    ]

                    Map responseMap = [
                            id                   : coupon["rule_id"].toString(),
                            dateCreated          : timeStamp,
                            lastUpdated          : timeStamp,
                            isDeleted            : false,
                            lastUpdatedBy        : null,
                            couponRuleinfo       : couponRuleinfo,
                            couponRuleAction     : couponRuleAction,
                            ruleNode             : null,
                            couponCodedetail     : couponCodedetail,
                            emailIds             : emailIds,
                            associateWithNewUsers: couponType == 'AUTO',
                            clickForOffers       : true,
                    ]

                    Map errorMap = validateCoupon(responseMap, couponHasCode)
                    if (errorMap) {
                        SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }

                        try {
                            query = hSessionInner.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
                            VALUES (:entity_id,:failure_reason,:progress_index_id,0)""")
                            query.with {
                                setLong("entity_id", errorMap["entityId"] as long)
                                setLong("progress_index_id", progressIndexId)
                                setString("failure_reason", errorMap["failureReason"])
                                executeUpdate()
                            }
                        }
                        catch (ConstraintViolationException e) {
                            log.error e.localizedMessage
                        }


                    } else {
                        SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_valid=count_valid+1,count_iteration_valid=count_iteration_valid+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }
                    }

                    couponList << responseMap

                }
                catch (Exception e) {
                    errorTrackingMessage = e.localizedMessage
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
            coupons.each { coupon ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ couponCallable(coupon, hSessionInner) } as Callable)
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)

            if (errorTrackingMessage) {
                throw new MigrationException("Worker threads are unable to process due to some reason")
            }

            if (couponUserList) {
                elasticSearchService.migrate(couponUserList, "coupon-user", "coupon-user", progressId, hSession, progressIndexId, false)
            }
            if (couponCodeList) {
                elasticSearchService.migrate(couponCodeList, "coupon-code", "coupon-code", progressId, hSession, progressIndexId, false)
            }
            elasticSearchService.migrate(couponList, "coupon", "coupon", progressId, hSession, progressIndexId)

            log.info("Starting next iteration")
            Long resumePoint = counter + coupons.size()
            counter += MigrationConstants.COUPON_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateCoupon(progressId, counter, couponCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getCouponsFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT sr.rule_id, sr.name,sr.description,sr.from_date,sr.to_date,sr.is_active,sr.sort_order,sr.coupon_type,
        sr.voucher_type,srl.label, sr.uses_per_coupon,sr.uses_per_customer,sr.times_used,sr.discount_amount,sr.discount_qty,sr.simple_free_shipping,sr.simple_action,
        sr.use_auto_generation,count(src.coupon_id) as couponCodeCount FROM salesrule sr LEFT JOIN salesrule_label srl ON sr.rule_id = srl.rule_id LEFT JOIN
        salesrule_coupon src ON sr.rule_id = src.rule_id group by sr.rule_id LIMIT :limit,:totalSize""")
        // WHERE sr.times_used>0
        def coupons = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.COUPON_ITERATION_SIZE)
            list()
        }
        coupons
    }

    def getCouponCodeFromDb(Integer ruleId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT coupon_id,code,customer_id,customer_email,created_at FROM salesrule_coupon WHERE rule_id= :rule_id")
        def code = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rule_id", ruleId)
            uniqueResult()
        }
        code
    }

    /*def getAutoGenCouponCodesFromDb(Integer ruleId, Session hSession, Long couponCodeCounter) {

        SQLQuery query = hSession.createSQLQuery("SELECT coupon_id,code,customer_id,created_at FROM salesrule_coupon WHERE rule_id= :rule_id LIMIT :limit,:totalSize") // and times_used > 0
        def autoGenCouponCodes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rule_id", ruleId)
            setLong("limit", couponCodeCounter)
            setLong("totalSize", MigrationConstants.COUPON_CODE_ITERATION_SIZE)
            list()
        }
        autoGenCouponCodes
    }*/

    def getAutoGenCouponCodesFromDb(Integer ruleId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT coupon_id,code,customer_id,created_at FROM salesrule_coupon WHERE rule_id= :rule_id and times_used > 0")
        def autoGenCouponCodes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("rule_id", ruleId)
            list()
        }
        autoGenCouponCodes
    }


    Map validateCoupon(Map coupon, boolean couponHasCode) {
        Map errorMap = null
        CouponCO co = new CouponCO()
        co.ruleName = coupon["couponRuleinfo"]["ruleName"]
        co.couponActionType = coupon["couponRuleAction"]["couponActionType"]
        co.value = coupon["couponRuleAction"]["value"]
        if (!co.validate()) { // || !validateCouponCode(coupon, couponHasCode)
            coupon["isDeleted"] = true
            coupon["couponRuleinfo"]["status"] = false
            String message
            /*if (!validateCouponCode(coupon, couponHasCode)) {
                message = "Coupon code(s) are null"
            } else {
                message = co.errors.allErrors.collect {
                    messageSource.getMessage(it, Locale.default)
                }.join("<br/>")
            }*/
            message = co.errors.allErrors.collect {
                messageSource.getMessage(it, Locale.default)
            }.join("<br/>")
            log.error "Validation failed for coupon : [${coupon.id}], message : ${message}"
            errorMap = [
                    entityId     : coupon.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }

    Boolean validateCouponCode(Map coupon, boolean couponHasCode) {
        if (coupon) {
            if (coupon["couponRuleinfo"]["couponType"] == "SPECIFIC_COUPON" && !couponHasCode) {
                return false
            } else if (coupon["couponRuleinfo"]["couponType"] == "AUTO" && !couponHasCode) {
                return false
            } else {
                return true
            }
        } else {
            return false
        }
    }
}
