package com.migrate

import com.migrate.co.UserCO
import com.migrate.enums.ProgressStatus
import com.migrate.exception.MigrationException
import groovy.transform.TailRecursive
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

class NewsLetterService {
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
            log.info("--------------------------------Starting newsletter migration--------------------------------------")
            hSession = sessionFactory.openSession()
            Long newsletterCount = (Long) hSession.createSQLQuery("SELECT count(distinct lower(subscriber_email)) FROM newsletter_subscriber where customer_id=0").list().first()
            log.info(newsletterCount + " total newsletter rows are found to migrate.")

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='NEWSLETTER'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < newsletterCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + newsletterCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", newsletterCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Newsletter")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "NEWSLETTER")
                    setLong("total_documents", newsletterCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='NEWSLETTER'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            Long timeStamp = new Date().time
            if (counter < newsletterCount) {
                populateNewsletter(progressId, counter, newsletterCount, timeStamp, hSession, progressIndexId)

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

            log.info("--------------------------------Ending newsletter migration-------------------------------------------")
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
    private Boolean populateNewsletter(Long progressId, Long counter, Long newsletterCount, Long timeStamp, Session hSession, Long progressIndexId) {

        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping newsletter migration-------------------------------------------")
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

        if (counter < newsletterCount) {
            processed = 0
            failed = 0
            succeeded = 0

            Date date1 = new Date()
            List newsletters = getNewsletterFromDb(counter, hSession)
            log.info("Fetched " + newsletters.size() + " newsletter from db to migrate in one iteration")
            List<Map> newsletterList = [].asSynchronized()
            String errorTrackingMessage = null

            def newsletterCallable = { newsletter, hSessionInner ->
                try {

                    Map newsletterMap = [
                            id                : newsletter["subscriber_id"].toString(),
                            emailId           : newsletter["subscriber_email"],
                            customerType      : "GUEST",
                            customerID        : null,
                            customerFirstName : newsletter["subscriber_email"].toString().tokenize('@')[0],
                            gender            : CommonService.customerGenderMap[newsletter["gender"].toString()],
                            customerLastName  : "",
                            subscriptionStatus: newsletter["subscriber_status"] == 1 ? "SUBSCRIBED" : "UNSUBSCRIBED",
                            subscribedOn      : timeStamp,
                            unsubscribedOn    : newsletter["subscriber_status"] == 3 ? timeStamp : null,
                            isDeleted         : false,
                            lastUpdatedBy     : '',
                            dateCreated       : timeStamp,
                            lastUpdated       : timeStamp,

                    ]

                    newsletterList << newsletterMap

                    synchronized (processedLock) {
                        processed++
                    }
                    synchronized (succeededLock) {
                        succeeded++
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
            newsletters.each { newsletter ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ newsletterCallable(newsletter, hSessionInner) } as Callable)
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
            log.info("Time to process " + newsletters.size() + " newsletters : " + (date2.time - date1.time) / 1000)

            Date date3 = new Date()
            elasticSearchService.migrate(newsletterList, "newsletter-subscription", "newsletter-subscription", progressId, hSession, progressIndexId)
            Date date4 = new Date()
            log.info("Time to index " + newsletters.size() + " newsletters : " + (date4.time - date3.time) / 1000)
            log.info("Starting next iteration")
            Long resumePoint = counter + newsletters.size()
            counter += MigrationConstants.NEWSLETTER_ITERATION_SIZE
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
            log.info("Total Time to complete " + newsletters.size() + " newsletters migration : " + (date5.time - date1.time) / 1000)
            populateNewsletter(progressId, counter, newsletterCount, timeStamp, hSession, progressIndexId)


        } else {
            true
        }
    }

    def getNewsletterFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT subscriber_id,lower(subscriber_email) as subscriber_email,subscriber_status,gender FROM newsletter_subscriber where customer_id=0 group by lower(subscriber_email)  LIMIT :limit,:totalSize")
        def newsletters = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.NEWSLETTER_ITERATION_SIZE)
            list()
        }
        newsletters
    }

}
