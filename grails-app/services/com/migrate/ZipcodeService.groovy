package com.migrate

import com.migrate.enums.ProgressStatus
import com.migrate.exception.MigrationException
import groovy.transform.TailRecursive
import migration.Progress
import migration.ProgressIndex
import org.hibernate.SQLQuery
import org.hibernate.Session
import org.hibernate.SessionFactory
import org.hibernate.transform.AliasToEntityMapResultTransformer

class ZipcodeService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService

    void migrate(Long progressId) {
        log.info("-----------------------------Starting Zipcode migration--------------------------------------------")
        Session hSession = sessionFactory.openSession()
        Long zipcodeCount = hSession.createSQLQuery("SELECT count(DISTINCT zipcodeimport_id) FROM fcm_zipcodeimport").list().first() as Long
        log.info(zipcodeCount + " many zipcodes are found to migrate")
        Long timeStamp = new Date().time

        Long counter = 0
        Long progressIndexId = null
        SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point FROM progress_index
                WHERE progress_id =:progress_id AND current_index='ZIPCODE'""")
        def progressIndexAttr = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("progress_id", progressId)
            uniqueResult()
        }

        if (progressIndexAttr) {
            counter = progressIndexAttr["resume_point"]
            progressIndexId = progressIndexAttr["id"]
            log.info("Resume migration process from index " + counter)
            log.info("Resetting processed count to " + counter)
            log.info("Updating total items count to " + zipcodeCount)
            query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents WHERE id =:id""")
            query.with {
                setLong("count_processed", counter)
                setLong("total_documents", zipcodeCount)
                setLong("id", progressIndexId)
                executeUpdate()
            }

        } else {
            log.info("Running fresh migration for Zipcodes")
            query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,current_index,total_documents,progress_start,status,count_processed,count_migrated,resume_point,version,error_message,formatted_percentage)
                VALUES (:progress_id,:current_index,:total_documents,:progress_start,:status,:count_processed,:count_migrated,:resume_point,0,'',0)""")
            query.with {
                setLong("progress_id", progressId)
                setString("current_index", "ZIPCODE")
                setLong("total_documents", zipcodeCount)
                setDate("progress_start", new Date())
                setString("status", ProgressStatus.IN_PROGRESS.toString())
                setInteger("count_processed", 0)
                setInteger("count_migrated", 0)
                setInteger("resume_point", 0)
                executeUpdate()
            }

            query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND current_index='ZIPCODE'")
            progressIndexId = query.with {
                setLong("progress_id", progressId)
                uniqueResult()
            }
        }
        hSession.flush()

        populateZipcode(progressId, counter, zipcodeCount, timeStamp, hSession, progressIndexId)

        query = hSession.createSQLQuery("""UPDATE progress_index
                SET progress_end =:progress_end, status =:status WHERE id =:id""")
        query.with {
            setDate("progress_end", new Date())
            setString("status", ProgressStatus.DONE.toString())
            setLong("id", progressIndexId)
            executeUpdate()
        }
        hSession.flush()
        hSession.close()
        log.info("-----------------------------Ending Zipcode migration--------------------------------------------")
    }

    @TailRecursive
    Boolean populateZipcode(Long progressId, Long counter, Long zipcodeCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping Zipcode migration-------------------------------------------")
            MigrateJob.isInterrupted = false
            throw new MigrationException("Migration is interrupted")
        }

        if (counter < zipcodeCount) {

            List zipcodes = getZipcodesFromDb(counter, hSession)
            log.info("Fetched " + zipcodes.size() + " many zipcodes from db to migrate in one iteration")
            List<Map> zipcodeList = []

            zipcodes.each { zipcode ->

                Map responseMap = [
                        id              : zipcode["zipcodeimport_id"].toString(),
                        zipcode         : zipcode["zip_code"],
                        state           : zipcode["state"],
                        city            : zipcode["city"],
                        express         : zipcode["express"],
                        standard        : zipcode["standard"],
                        appointment     : zipcode["appointment"],
                        overnite        : zipcode["overnite"],
                        cod             : zipcode["cod"],
                        shippingProvider: zipcode["carrier_name"],
                        expectedDays    : "",
                        codCharges      : 0.0,
                        shippingCharges : 0.0,
                        regionCode      : "",
                ]

                SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET count_processed =count_processed+1 WHERE id =:id""")
                query.with {
                    setLong("id", progressIndexId)
                    executeUpdate()
                }

                zipcodeList << responseMap

            }
            elasticSearchService.migrate(zipcodeList, "zipcode", "zipcode", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            counter += 1000
            log.info("Setting resume flag at Index " + counter)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", counter)
                executeUpdate()
            }
            hSession.flush()

            populateZipcode(progressId, counter, zipcodeCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getZipcodesFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT fz.*,fs.carrier_name FROM fcm_zipcodeimport fz LEFT JOIN fcm_shippingcarriers fs ON fs.blinkecarrier_id = fz.blinkecarrier_id LIMIT :limit,1000")
        def zipcodes = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            list()
        }
        zipcodes
    }
}
