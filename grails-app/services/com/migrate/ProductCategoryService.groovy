package com.migrate

import com.migrate.co.CategoryCO
import com.migrate.co.ReviewCO
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

class ProductCategoryService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    CommonService commonService
    def messageSource

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Category migration--------------------------------------------")

            hSession = sessionFactory.openSession()

            Long categoryCount = (Long) hSession.createSQLQuery("SELECT count(DISTINCT entity_id) FROM catalog_category_entity").list().first()
            log.info(categoryCount + " many categories are found to migrate")

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='CATEGORY'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < categoryCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + categoryCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", categoryCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }


            } else {
                log.info("Running fresh migration for Categories")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "CATEGORY")
                    setLong("total_documents", categoryCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='CATEGORY'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            Long timeStamp = new Date().time
            if (counter < categoryCount) {
                populateCategory(progressId, counter, categoryCount, timeStamp, hSession, progressIndexId)

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

            log.info("--------------------------------Ending category migration-------------------------------------------")
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
    private Boolean populateCategory(Long progressId, Long counter, Long categoryCount, Long timeStamp, Session hSession, Long progressIndexId) {

        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping category migration-------------------------------------------")
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

        if (counter < categoryCount) {

            List categories = getCategoriesFromDb(counter, hSession)
            log.info("Fetched " + categories.size() + " many categories from db to migrate in one iteration")
            List categoryList = [].asSynchronized()
            String errorTrackingMessage = null

            def categoryCallable = { category, hSessionInner ->

                try {
                    if (category["entity_id"] != 1 && category["entity_id"] != 2) {
                        def contains = {
                            ["46", "47", "48", "208", "209", "43", "57"].contains(it.toString())
                        }

                        Map responseMap = [
                                id                         : category["entity_id"].toString(),
                                name                       : null,
                                displayName                : null,
                                description                : null,
                                parentCategoryId           : null,
                                activeFrom                 : null,
                                activeTo                   : null,
                                dateCreated                : category["created_at"] ? category["created_at"].time : timeStamp,
                                lastUpdated                : category["updated_at"] ? category["updated_at"].time : timeStamp,
                                lastUpdatedBy              : null,
                                defaultSortOrder           : "BEST_VALUE_INDEX",
                                allowedSortingOptions      : ["PRICE", "DISCOUNT", "ARRIVAL"],
                                doesLandingPageExist       : false,
                                isPersonalizationConfigured: false,
                                isDeleted                  : false,
                                isActive                   : false,
                                isVisible                  : false,
                                seoData                    : [redirectionType: null, heading1: null, heading2: null, webURL: null, webUrlPath: null],
                                showFilterByGender         : false,
                                widget                     : null,
                        ]

                        if (category["parent_id"] && category["parent_id"] != 0 && category["parent_id"] != 1 && category["parent_id"] != 2) {
                            responseMap.parentCategoryId = category["parent_id"].toString()
                        }
                        List attributes = getAttributes(category["entity_id"], hSessionInner)
                        List attributesWithoutSEO = attributes.findAll { !contains(it["attribute_id"]) }?.grep()
                        List seoAttributes = attributes.findAll { contains(it["attribute_id"]) }?.grep()

                        responseMap = commonService.addAttributes(responseMap, attributesWithoutSEO)
                        responseMap["seoData"] = commonService.addAttributes(responseMap["seoData"], seoAttributes)
                        responseMap = postProcessing(responseMap, hSessionInner)

                        Map errorMap = validateCategory(responseMap)
                        if (errorMap) {
                            SQLQuery query2 = hSessionInner.createSQLQuery("""UPDATE progress_index
                            SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id = :id""")
                            query2.with {
                                setLong("id", progressIndexId)
                                executeUpdate()
                            }

                            try {
                                query2 = hSessionInner.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
                            VALUES (:entity_id,:failure_reason,:progress_index_id,0)""")
                                query2.with {
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
                            SQLQuery query2 = hSessionInner.createSQLQuery("""UPDATE progress_index SET count_processed=count_processed+1, count_valid=count_valid+1,
                            count_iteration_valid=count_iteration_valid+1 WHERE id = :id""")
                            query2.with {
                                setLong("id", progressIndexId)
                                executeUpdate()
                            }
                        }


                        categoryList << responseMap
                    }
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
            categories.each { category ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ categoryCallable(category, hSessionInner) } as Callable)
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)

            if (errorTrackingMessage) {
                throw new MigrationException("Worker threads are unable to process due to some reason")
            }

            elasticSearchService.migrate(categoryList, "category", "category", progressId, hSession, progressIndexId)

            log.info("Starting next iteration")
            Long resumePoint = counter + categories.size()
            counter += MigrationConstants.CATEGORY_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateCategory(progressId, counter, categoryCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getCategoriesFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT entity_id, parent_id, created_at, updated_at FROM catalog_category_entity LIMIT :limit,:totalSize")
        def categories = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.CATEGORY_ITERATION_SIZE)
            list()
        }
        categories
    }

    List getAttributes(Integer id, Session hSession) {
        SQLQuery query = hSession.createSQLQuery("SELECT attribute_id, value FROM catalog_category_entity_varchar WHERE entity_id=:id AND value IS NOT NULL\n" +
                "UNION\n" +
                "SELECT attribute_id, value FROM catalog_category_entity_text WHERE entity_id=:id AND value IS NOT NULL\n" +
                "UNION\n" +
                "SELECT attribute_id, value FROM catalog_category_entity_int WHERE entity_id=:id AND value IS NOT NULL")
        List attributes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setInteger("id", id)
            list()
        }
        attributes
    }

    Map postProcessing(Map responseMap, Session hSession) {
        responseMap["displayName"] = responseMap["name"]
        if (CommonService.personalizationConfiguredCategoryList.contains(responseMap["id"])) {
            responseMap["isPersonalizationConfigured"] = true
        }
        if (responseMap["name"].toString().equalsIgnoreCase("men") || responseMap["name"].toString().equalsIgnoreCase("women")) {
            responseMap["doesLandingPageExist"] = true
        }
        List additionalInputs = [responseMap["displayName"]]?.grep()
        responseMap["suggest"] = CommonUtils.createSuggestions(responseMap["name"], responseMap["name"], responseMap["id"], 0, additionalInputs)

        responseMap["childCategoryIds"] = []
        responseMap["childCategoryIds"] = getChildCategories(responseMap["id"] as int, hSession)
        responseMap
    }

    List getChildCategories(Integer id, Session hSession) {
        SQLQuery query = hSession.createSQLQuery("SELECT DISTINCT entity_id FROM catalog_category_entity WHERE parent_id =:id")
        List categoryIds = query.with {
            setInteger("id", id)
            list()
        }.collect { it.toString() }
        categoryIds
    }

    Map validateCategory(Map category) {
        Map errorMap = null
        CategoryCO co = new CategoryCO()
        DataBindingUtils.bindObjectToInstance(co, category, [], [], null)
        if (!co.validate()) {
            category["isDeleted"] = true
            category["isActive"] = false
            category["isVisible"] = false
            String message = co.errors.allErrors.collect {
                messageSource.getMessage(it, null)
            }.join("<br/>")
            log.error "Validation failed for category : [${category.id}], message : ${message}"
            errorMap = [
                    entityId     : category.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }

}
