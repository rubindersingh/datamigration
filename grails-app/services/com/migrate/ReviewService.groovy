package com.migrate

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

class ReviewService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    def messageSource

    void migrate(Long progressId) {
        Session hSession = null
        try{
            log.info("-----------------------------Starting Review migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long reviewCount = hSession.createSQLQuery("SELECT count(DISTINCT review_id) FROM review").list().first() as Long
            log.info(reviewCount + " many reviews are found to migrate")
            Long timeStamp = new Date().time

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='REVIEW'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < reviewCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + reviewCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", reviewCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Reviews")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "REVIEW")
                    setLong("total_documents", reviewCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='REVIEW'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            if (counter < reviewCount) {
                populateReview(progressId, counter, reviewCount, timeStamp, hSession, progressIndexId)

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

            log.info("-----------------------------Ending Review migration--------------------------------------------")
        }
        catch (Exception e)
        {
            throw e
        }
        finally {
            if(hSession){
                hSession.flush()
                hSession.close()
            }
        }
    }

    @TailRecursive
    Boolean populateReview(Long progressId, Long counter, Long reviewCount, Long timeStamp, Session hSession, Long progressIndexId) {

        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping review migration-------------------------------------------")
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

        if (counter < reviewCount) {

            List reviews = getReviewsFromDb(counter, hSession)
            log.info("Fetched " + reviews.size() + " many reviews from db to migrate in one iteration")
            List<Map> reviewList = [].asSynchronized()
            String errorTrackingMessage = null

            def reviewCallable = { review, hSessionInner ->

                try {

                    Map responseMap = [
                            id           : review["review_id"].toString(),
                            userName     : review["nickname"],
                            location     : review["location"],
                            title        : review["title"],
                            comment      : review["detail"],
                            email        : review["email"],
                            userType     : review["customer_id"] != null ? "normal" : "guest",
                            productName  : review["productName"],
                            productSKU   : review["sku"],
                            isDeleted    : false,
                            dateCreated  : review["created_at"].time,
                            lastUpdatedBy: "",
                            lastUpdated  : review["created_at"].time,
                            status       : CommonService.statusMap[review["status_id"].toString()],
                            rating       : review["rating"] ? review["rating"] * 20 : 20,
                    ]

                    Map errorMap = validateReview(responseMap)
                    if (errorMap) {
                        SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }

                        try
                        {
                            query = hSessionInner.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
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
                        SQLQuery query = hSessionInner.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_valid=count_valid+1,count_iteration_valid=count_iteration_valid+1 WHERE id =:id""")
                        query.with {
                            setLong("id", progressIndexId)
                            executeUpdate()
                        }
                    }

                    reviewList << responseMap
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

            ExecutorService pool = Executors.newFixedThreadPool(1000);
            reviews.each { review ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ reviewCallable(review, hSessionInner) } as Callable)
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)

            if(errorTrackingMessage){
                throw new MigrationException("Worker threads are unable to process due to some reason")
            }

            elasticSearchService.migrate(reviewList, "review", "review", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            Long resumePoint = counter+reviews.size()
            counter += MigrationConstants.REVIEW_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateReview(progressId, counter, reviewCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getReviewsFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT
                          r.review_id,
                          rd.nickname,
                          rd.location,
                          rd.title,
                          rd.detail,
                          rd.email,
                          rd.customer_id,
                          cpev.value AS productName,
                          cpe.sku,
                          r.created_at,
                          r.status_id,
                          rov.value  AS rating
                        FROM review r
                          LEFT JOIN review_detail rd ON r.review_id = rd.review_id
                          LEFT JOIN catalog_product_entity cpe ON r.entity_pk_value = cpe.entity_id
                          LEFT JOIN catalog_product_entity_varchar cpev ON cpe.entity_id = cpev.entity_id
                          LEFT JOIN rating_option_vote rov ON r.review_id = rov.review_id
                        WHERE cpev.attribute_id = 71 AND cpev.store_id=0
                        LIMIT :limit,:totalSize""")
        def reviews = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.REVIEW_ITERATION_SIZE)
            list()
        }
        reviews
    }

    Map validateReview(Map review) {
        Map errorMap = null
        ReviewCO co = new ReviewCO()
        DataBindingUtils.bindObjectToInstance(co, review, ["id", "userName", "status", "rating", "location", "title", "comment", "email", "userType", "productName", "productSKU"], [], null)
        if (!co.validate()) {
            review["isDeleted"] = true
            review["status"] = CommonService.statusMap["2"]
            String message = co.errors.allErrors.collect {
                messageSource.getMessage(it, null)
            }.join("<br/>")
            log.error "Validation failed for review : [${co.id}], message : ${message}"
            errorMap = [
                    entityId     : co.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }
}
