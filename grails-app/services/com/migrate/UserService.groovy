package com.migrate

import com.migrate.co.UserCO
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

class UserService {
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
            log.info("--------------------------------Starting user migration--------------------------------------")
            hSession = sessionFactory.openSession()
            Long userCount = (Long) hSession.createSQLQuery("SELECT count(DISTINCT entity_id) FROM customer_entity").list().first()
            log.info(userCount + " many total user rows are found to migrate.")

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='USER'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < userCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + userCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", userCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Users")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "USER")
                    setLong("total_documents", userCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='USER'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            Long timeStamp = new Date().time
            if (counter < userCount) {
                populateUser(progressId, counter, userCount, timeStamp, hSession, progressIndexId)

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

            log.info("--------------------------------Ending user migration-------------------------------------------")
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
    private Boolean populateUser(Long progressId, Long counter, Long userCount, Long timeStamp, Session hSession, Long progressIndexId) {

        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping user migration-------------------------------------------")
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

        if (counter < userCount) {
            processed = 0
            failed = 0
            succeeded = 0



            Date date1 = new Date()
            List users = getUsersFromDb(counter, hSession)
            log.info("Fetched " + users.size() + " many users from db to migrate in one iteration")
            List<Map> userList = [].asSynchronized()
            List<Map> newsletterList = [].asSynchronized()
            String errorTrackingMessage = null

            def userCallable = { user, hSessionInner ->
                try {
                    List userAttributes = getuserAttributes(user["entity_id"], hSessionInner)
                    List userAddresses = getUserAddresses(user["entity_id"], hSessionInner)
                    List userReferrals = getUserReferrals(user["entity_id"], hSessionInner)
                    List userStoreCredits = getUserStoreCredits(user["entity_id"], hSessionInner)
                    Map userSubscription = getUserSubscriptionFromDb(user["entity_id"], user["email"], hSessionInner)
                    Boolean sendEmailUpdates = false
                    if (userSubscription) {
                        sendEmailUpdates = userSubscription["subscriber_status"] == 1
                    }

                    Map responseMap = [
                            id                      : user["entity_id"].toString(),
                            emailId                 : user["email"],
                            title                   : null,
                            firstName               : null,
                            lastName                : null,
                            mobileNumber            : null,
                            gender                  : null,
                            zipCode                 : null,
                            country                 : null,
                            state                   : null,
                            city                    : null,
                            dateOfBirth             : null,
                            password                : null,
                            customerSince           : CommonUtils.convertTimeStamptoDate(user["created_at"]),
                            lastUpdatedBy           : null,
                            maritalStatus           : null,
                            sendEmailUpdates        : sendEmailUpdates,
                            enabled                 : user["is_active"] ? true : false,
                            isBackendUser           : false,
                            isPasswordChangeRequired: false,
                            accountExpired          : false,
                            accountLocked           : false,
                            passwordExpired         : false,
                            isDeleted               : false,
                            isSuperAdmin            : false,
                            isExistingThirdPartyUser: null,
                            isBlocked               : false,
                            dateCreated             : user["created_at"] ? user["created_at"].time : timeStamp,
                            lastUpdated             : user["updated_at"] ? user["updated_at"].time : timeStamp,
                            passwordChangedDate     : null,
                            referrals               : [],
                            storeCredits            : [],
                            authenticationTokens    : null,
                            addresses               : [],
                            utmData                 : null,
                    ]

                    responseMap = commonService.addAttributes(responseMap, userAttributes)

                    Map utmData = [
                            source  : null,
                            campaign: null,
                            medium  : null,
                            term    : null,
                            content : null,
                            page_url: null,
                    ]
                    responseMap["utmData"] = commonService.addUserUTMAttributes(utmData, userAttributes)

                    responseMap = mergeMiddleName(responseMap)


                    userAddresses?.each { address ->
                        def userAddressAttributes = getAddressAttributes(address["entity_id"], hSessionInner)

                        Map addressMap = [
                                id           : address["entity_id"].toString(),
                                nickName     : null,
                                title        : null,
                                firstName    : null,
                                lastName     : null,
                                mobileNumber : null,
                                city         : null,
                                pinCode      : null,
                                country      : null,
                                state        : null,
                                addressLine1 : null,
                                addressLine2 : null,
                                type         : null,
                                isDefault    : false,
                                isDeleted    : false,
                                dateCreated  : address["created_at"] ? address["created_at"].time : timeStamp,
                                lastUpdatedBy: '',
                                lastUpdated  : address["updated_at"] ? address["updated_at"].time : timeStamp,
                        ]

                        addressMap = commonService.addAttributes(addressMap, userAddressAttributes)
                        addressMap = mergeMiddleName(addressMap)
                        responseMap["addresses"] << addressMap
                    }
                    userReferrals?.each { referral ->
                        Map referralMap = [
                                emailId               : referral["email"],
                                date                  : referral["invitation_date"] ? referral["invitation_date"].time : timeStamp,
                                referralDeliveryStatus: "ERROR",
                                isDeleted             : false,
                                dateCreated           : referral["invitation_date"] ? referral["invitation_date"].time : timeStamp,
                                lastUpdated           : referral["invitation_date"] ? referral["invitation_date"].time : timeStamp,
                                lastUpdatedBy         : null,
                        ]

                        responseMap["referrals"] << referralMap
                    }

                    userStoreCredits?.each { storeCredit ->
                        Map storeCreditMap = [
                                id                   : storeCredit["id"].toString(),
                                dateCreated          : storeCredit["dateCreated"] ? storeCredit["dateCreated"].time : timeStamp,
                                lastUpdated          : storeCredit["lastUpdated"] ? storeCredit["lastUpdated"].time : timeStamp,
                                storeCreditActionType: storeCredit["storeCreditActionType"],
                                balancePoints        : convertValueToFloat(storeCredit["balancePoints"]),
                                balancePointsInCash  : convertValueToFloat(storeCredit["balancePointsInCash"]),
                                previousBalanceInCash: convertValueToFloat(storeCredit["previousBalanceInCash"]),
                                additionalInformation: storeCredit["additionalInformation"],
                                isCustomerNotified   : (storeCredit["isCustomerNotified"] == 1),
                                isDeleted            : false,
                                orderNumber          : getOrderNumberForStoreCredit(storeCredit["additionalInformation"].toString()),
                                messageTrackingId    : null,
                                lastUpdatedBy        : null,
                                storeCreditRate      : 1.0,
                        ]

                        responseMap["storeCredits"] << storeCreditMap
                    }

                    if (userSubscription) {
                        String gender = CommonService.customerGenderMap[userSubscription["gender"].toString()]
                        if (!gender) {
                            gender = responseMap["gender"]
                        }

                        Map newsletterMap = [
                                id                : userSubscription["subscriber_id"].toString(),
                                emailId           : userSubscription["subscriber_email"],
                                customerType      : "CUSTOMER",
                                customerID        : userSubscription["customer_id"],
                                customerFirstName : responseMap["firstName"],
                                gender            : gender,
                                customerLastName  : responseMap["lastName"],
                                subscriptionStatus: userSubscription["subscriber_status"] == 1 ? "SUBSCRIBED" : "UNSUBSCRIBED",
                                subscribedOn      : user["created_at"] ? user["created_at"].time : timeStamp,
                                unsubscribedOn    : userSubscription["subscriber_status"] == 3 ? timeStamp : null,
                                isDeleted         : false,
                                lastUpdatedBy     : '',
                                dateCreated       : user["created_at"] ? user["created_at"].time : timeStamp,
                                lastUpdated       : user["created_at"] ? user["created_at"].time : timeStamp,

                        ]

                        newsletterList << newsletterMap
                    }

                    Map errorMap = validateUser(responseMap)
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
                            log.error e.localizedMessage
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

                    userList << responseMap
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
            users.each { user ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ userCallable(user, hSessionInner) } as Callable)
            }
            pool.shutdown();
            log.info("Waiting for threads termination")
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
            log.info("Time to process " + users.size() + " users : " + (date2.time - date1.time) / 1000)

            populateUserRoles(userList, hSession, progressId, progressIndexId, counter, timeStamp)
            populateUserCart(userList, hSession, progressId, progressIndexId, counter)
            Date date3 = new Date()
            log.info("---------------Indexing newsletter start--------------------------")
            if (newsletterList) {
                elasticSearchService.migrate(newsletterList, "newsletter-subscription", "newsletter-subscription", progressId, hSession, progressIndexId, false)
            }
            log.info("---------------Indexing user start--------------------------")
            elasticSearchService.migrate(userList, "user", "user", progressId, hSession, progressIndexId)
            Date date4 = new Date()
            log.info("Time to index " + users.size() + " users : " + (date4.time - date3.time) / 1000)
            log.info("Starting next iteration")
            Long resumePoint = counter + users.size()
            counter += MigrationConstants.USER_ITERATION_SIZE
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
            log.info("Total Time to complete " + users.size() + " users migration : " + (date5.time - date1.time) / 1000)
            populateUser(progressId, counter, userCount, timeStamp, hSession, progressIndexId)


        } else {
            true
        }
    }

    Map mergeMiddleName(Map responseMap) {
        if (responseMap["middleName"]) {
            responseMap["lastName"] = responseMap["middleName"] + " " + responseMap["lastName"]
        }
        responseMap.remove("middleName")
        responseMap
    }

    Map validateUser(Map user) {
        Map errorMap = null
        UserCO co = new UserCO()
        DataBindingUtils.bindObjectToInstance(co, user, ["id", "emailId"], [], null)
        if (!co.validate()) {
            user["enabled"] = false
            user["isDeleted"] = true
            String message = co.errors.allErrors.collect {
                messageSource.getMessage(it, null)
            }.join("<br/>")
            log.error "Validation failed for user : [${co.id} | ${co.emailId}], message : ${message}"
            errorMap = [
                    entityId     : co.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }

    void populateUserRoles(List userList, Session hSession, Long progressId, Long progressIndexId, Long counter, Long timeStamp) {
        log.info("-----------------------------Migrating User Roles Start--------------------------")
        Integer count = counter;
        List roles = userList?.collect { def user ->
            [
                    id           : (count++).toString(),
                    userId       : user["id"],
                    roleId       : user["emailId"].contains("admin") ? 2 : 1,
                    isDeleted    : false,
                    dateCreated  : user["created_at"] ? user["created_at"].time : timeStamp,
                    lastUpdated  : user["updated_at"] ? user["updated_at"].time : timeStamp,
                    lastUpdatedBy: null,

            ]
        }
        elasticSearchService.migrate(roles, "user-role", "user-role", progressId, hSession, progressIndexId, false)
        log.info("-----------------------------Migration User Roles End--------------------------")
    }

    void populateUserCart(List userList, Session hSession, Long progressId, Long progressIndexId, Long counter) {
        log.info("-----------------------------Migrating User Carts Start--------------------------")
        Integer count = counter;
        List carts = userList?.collect { def user ->
            [
                    id           : (count++).toString(),
                    lineItems    : [],
                    userId       : user["id"],
                    couponCode   : null,
                    isDeleted    : false,
                    dateCreated  : new Date().time,
                    lastUpdated  : new Date().time,
                    lastUpdatedBy: null,
            ]
        }
        elasticSearchService.migrate(carts, "cart", "cart", progressId, hSession, progressIndexId, false)
        log.info("-----------------------------Migration User Carts End--------------------------")
    }

    def getUsersFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT entity_id, email, created_at, updated_at, is_active FROM customer_entity LIMIT :limit,:totalSize")
        def users = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.USER_ITERATION_SIZE)
            list()
        }
        users
    }

    def getUserSubscriptionFromDb(Integer id, String email, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT subscriber_id,customer_id,subscriber_email,subscriber_status,gender FROM newsletter_subscriber WHERE customer_id =:userId AND lower(subscriber_email)=:email order by subscriber_status asc,gender desc")
        def subscriptions = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("userId", id)
            setString("email", email.toLowerCase())
            list()
        }
        subscriptions ? subscriptions.first() : [:]
    }

    List getuserAttributes(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT attribute_id, value FROM customer_entity_datetime ce WHERE ce.entity_id=:userId
        UNION
        SELECT attribute_id, value FROM customer_entity_varchar ce WHERE ce.entity_id=:userId
        UNION
        SELECT attribute_id, value FROM customer_entity_int ce WHERE ce.entity_id=:userId""")
        def userAttributes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("userId", id)
            list()
        }
        userAttributes
    }

    List getUserAddresses(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT entity_id, created_at, updated_at, is_active FROM customer_address_entity ce WHERE ce.parent_id=:userId")
        def userAddresses = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("userId", id)
            list()
        }
        userAddresses
    }

    List getAddressAttributes(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT attribute_id, value FROM customer_address_entity_varchar ce WHERE ce.entity_id =:addressId\n" +
                "UNION\n" +
                "SELECT attribute_id, value FROM customer_address_entity_text ce WHERE ce.entity_id =:addressId")
        def userAddressAttributes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("addressId", id)
            list()
        }
        userAddressAttributes
    }

    List getUserReferrals(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT email, invitation_date, status FROM enterprise_invitation WHERE customer_id =:userId")
        def userReferrals = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("userId", id)
            list()
        }
        userReferrals
    }

    List getUserStoreCredits(Integer id, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT
        ech.history_id AS id,ech.updated_at AS dateCreated,ech.updated_at AS lastUpdated,
        FALSE AS isDeleted,if(ech.action = 1,"UPDATED",if(ech.action = 2,"CREATED",if(ech.action = 3,"USED",if(ech.action = 4,"REFUNDED","REVERTED")))) AS storeCreditActionType,
        ech.balance_delta AS balancePoints,
        ech.balance_delta AS balancePointsInCash,
        (ech.balance_amount - ech.balance_delta) AS previousBalanceInCash,
         additional_info AS additionalInformation,
        if(is_customer_notified = 1,TRUE ,FALSE) AS isCustomerNotified
         FROM enterprise_customerbalance ec
        RIGHT OUTER JOIN enterprise_customerbalance_history ech ON ec.balance_id = ech.balance_id
        WHERE ec.customer_id =:userId ORDER BY ech.updated_at""")
        def userStoreCredits = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("userId", id)
            list()
        }
        userStoreCredits
    }

    Float convertValueToFloat(def value) {
        Float val = 0
        if (value) {
            try {
                val = value.toString().toFloat()
            } catch (Exception ignored) {
                val = 0
            }
        }
        val
    }

    String getOrderNumberForStoreCredit(String info) {
        String orderNumber = ""
        if (info) {
            List infoList = info.tokenize(" ")
            if (infoList.size() > 1 && infoList[0].toString().equalsIgnoreCase("order") && infoList[1].toString().startsWith("#")) {
                String incrementId = infoList[1].toString().replaceAll("[^0-9A-Za-z]", "")
                if (incrementId) {
                    orderNumber = incrementId
                }
            }
        }
        orderNumber
    }
}
