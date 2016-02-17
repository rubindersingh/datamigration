package com.migrate

import com.migrate.co.TestimonialCO
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

class TestimonialService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    def messageSource

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Testimonial migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long testimonialCount = hSession.createSQLQuery("SELECT count(DISTINCT testimonials_id) FROM testimonials").list().first() as Long
            log.info(testimonialCount + " many testimonials are found to migrate")
            Long timeStamp = new Date().time

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='TESTIMONIAL'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < testimonialCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + testimonialCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", testimonialCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Testimonials")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "TESTIMONIAL")
                    setLong("total_documents", testimonialCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='TESTIMONIAL'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            if (counter < testimonialCount) {
                populateTestimonial(progressId, counter, testimonialCount, timeStamp, hSession, progressIndexId)

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

            log.info("-----------------------------Ending Testimonial migration--------------------------------------------")
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
    Boolean populateTestimonial(Long progressId, Long counter, Long testimonialCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping testimonial migration-------------------------------------------")
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

        if (counter < testimonialCount) {

            List testimonials = getTestimonialsFromDb(counter, hSession)
            log.info("Fetched " + testimonials.size() + " many testimonials from db to migrate in one iteration")
            List<Map> testimonialList = []
            Map ratingMap = [
                    "1": "POOR",
                    "2": "AVERAGE",
                    "3": "GOOD",
                    "4": "EXCELLENT",
            ]

            testimonials.each { testimonial ->

                List name = testimonial["name"].toString().tokenize(" ")
                def firstName = name[0]
                def lastName = ""
                if (name.size() > 1) {
                    name.remove(0)
                    lastName = name.join(" ")
                }

                Map responseMap = [
                        id                    : testimonial["testimonials_id"].toString(),
                        firstName             : firstName,
                        lastName              : lastName,
                        place                 : testimonial["place"],
                        email                 : testimonial["email"],
                        content               : testimonial["content"],
                        isDeleted             : false,
                        dateCreated           : testimonial["created_time"].time,
                        lastUpdatedBy         : null,
                        lastUpdated           : testimonial["update_time"].time,
                        productRating         : ratingMap[testimonial["rating_product"].toString()],
                        customerServiceRating : ratingMap[testimonial["rating_service"].toString()],
                        brandServiceRating    : ratingMap[testimonial["rating_brand"].toString()],
                        onlineExperienceRating: ratingMap[testimonial["rating_website"].toString()],
                        status                : testimonial["status"] == 1,
                ]

                Map errorMap = validateTestimonial(responseMap)
                if (errorMap) {
                    SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_fail=count_fail+1,count_iteration_fail=count_iteration_fail+1 WHERE id =:id""")
                    query.with {
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }

                    try {
                        query = hSession.createSQLQuery("""INSERT INTO index_failure_info(entity_id,failure_reason,progress_index_id,version)
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
                    SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                        SET count_processed=count_processed+1, count_valid=count_valid+1,count_iteration_valid=count_iteration_valid+1 WHERE id =:id""")
                    query.with {
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

                testimonialList << responseMap

            }
            elasticSearchService.migrate(testimonialList, "testimonial", "testimonial", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            Long resumePoint = counter + testimonials.size()
            counter += MigrationConstants.TESTIMONIAL_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateTestimonial(progressId, counter, testimonialCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getTestimonialsFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT testimonials_id,name,email,place,content,created_time,update_time," +
                "rating_product,rating_service,rating_brand,rating_website,status FROM testimonials LIMIT :limit,:totalSize")
        def testimonials = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.TESTIMONIAL_ITERATION_SIZE)
            list()
        }
        testimonials
    }

    Map validateTestimonial(Map testimonial) {
        Map errorMap = null
        TestimonialCO co = new TestimonialCO()
        DataBindingUtils.bindObjectToInstance(co, testimonial, ["firstName", "lastName", "email", "place", "content"], [], null)
        if (!co.validate()) {
            testimonial["isDeleted"] = true
            testimonial["status"] = false
            String message = co.errors.allErrors.collect {
                messageSource.getMessage(it, null)
            }.join("<br/>")
            log.error "Validation failed for testimonial : [${testimonial.id}] message : ${message}"
            errorMap = [
                    entityId     : testimonial.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }
}
