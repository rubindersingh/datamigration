package com.migrate

import com.migrate.co.DiscountRuleActionCO
import com.migrate.co.DiscountRuleInfoCO
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

class DiscountService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    def messageSource

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Discount migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long discountRuleCount = hSession.createSQLQuery("SELECT count(DISTINCT rule_id) FROM catalogrule").list().first() as Long
            log.info(discountRuleCount + " many discount rules are found to migrate")
            Long timeStamp = new Date().time

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='DISCOUNT'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < discountRuleCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + discountRuleCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", discountRuleCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }
            } else {
                log.info("Running fresh migration for discounts")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "DISCOUNT")
                    setLong("total_documents", discountRuleCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='DISCOUNT'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()
            if (counter < discountRuleCount) {
                populateDiscount(progressId, counter, discountRuleCount, timeStamp, hSession, progressIndexId)

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

            log.info("-----------------------------Ending Discount migration--------------------------------------------")
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
    Boolean populateDiscount(Long progressId, Long counter, Long discountRuleCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping discount migration-------------------------------------------")
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

        if (counter < discountRuleCount) {

            List discounts = getDiscountsFromDb(counter, hSession)
            log.info("Fetched " + discounts.size() + " many discounts from db to migrate in one iteration")
            List<Map> discountList = []
            Map actionConditionMap = [
                    "by_percent": "BY_PERCENTAGE",
                    "by_fixed"  : "BY_AMOUNT"
            ]

            discounts.each { discount ->

                Map discountRuleInfo = [
                        ruleName   : discount["name"],
                        description: discount["description"],
                        fromDate   : CommonUtils.convertTimeStamptoDate(discount["from_date"]),
                        toDate     : CommonUtils.convertTimeStamptoDate(discount["to_date"]),
                        status     : discount["is_active"] == 1,
                        priority   : discount["sort_order"],
                ]

                Map discountRuleAction = [
                        overrideProductDiscount  : false,
                        stopFurtherRuleProcessing: discount["stop_rules_processing"] == 1,
                        value                    : discount["discount_amount"].toString().toDouble(),
                        actionType               : actionConditionMap[discount["simple_action"]],
                ]

                Map responseMap = [
                        id                : discount["rule_id"].toString(),
                        dateCreated       : timeStamp,
                        lastUpdated       : timeStamp,
                        isDeleted         : false,
                        lastUpdatedBy     : null,
                        discountRuleAction: discountRuleAction,
                        discountRuleInfo  : discountRuleInfo,
                        ruleNode          : null,
                ]

                Map errorMap = validateDiscount(responseMap)
                if (errorMap) {
                    SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id =:id""")
                    query.with {
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }

                    try
                    {
                        query = hSession.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
                            VALUES (:entity_id,:failure_reason,:progress_index_id,0)""")
                        query.with {
                            setLong("entity_id", errorMap["entityId"] as long)
                            setLong("progress_index_id", progressIndexId)
                            setString("failure_reason", errorMap["failureReason"])
                            executeUpdate()
                        }
                    }
                    catch (ConstraintViolationException e){
                        log.error e.localizedMessage
                    }


                } else {
                    SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_valid=count_valid+1,count_iteration_valid=count_iteration_valid+1 WHERE id =:id""")
                    query.with {
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

                discountList << responseMap

            }
            elasticSearchService.migrate(discountList, "discount", "discount", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            Long resumePoint = counter+discounts.size()
            counter += MigrationConstants.DISCOUNT_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateDiscount(progressId, counter, discountRuleCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getDiscountsFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT cr.rule_id,cr.stop_rules_processing," +
                "cr.simple_action,cr.discount_amount,cr.name,cr.description," +
                "cr.from_date,cr.to_date,cr.is_active,cr.sort_order FROM catalogrule cr LIMIT :limit,:totalSize")
        def discounts = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.DISCOUNT_ITERATION_SIZE)
            list()
        }
        discounts
    }

    Map validateDiscount(Map discount) {
        Map errorMap = null
        DiscountRuleActionCO discountRuleActionCO = new DiscountRuleActionCO()
        DiscountRuleInfoCO discountRuleInfoCO = new DiscountRuleInfoCO()

        DataBindingUtils.bindObjectToInstance(discountRuleInfoCO, discount.discountRuleInfo, ["ruleName"], [], null)
        DataBindingUtils.bindObjectToInstance(discountRuleActionCO, discount.discountRuleAction, ["value", "actionType"], [], null)

        if (!discountRuleInfoCO.validate() || !discountRuleActionCO.validate()) {
            discount["isDeleted"] = true
            discount["discountRuleInfo"]["status"] = false
            String message = discountRuleInfoCO.errors?.allErrors?.collect {
                messageSource.getMessage(it, null)
            }?.join("/n")
            message << discountRuleActionCO.errors?.allErrors?.collect {
                messageSource.getMessage(it, null)
            }?.join("/n")
            log.error "Validation failed for discount : [${discount.id} | ${discountRuleInfoCO.ruleName}], message : ${message}"
            errorMap = [
                    entityId     : discount.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }
}
