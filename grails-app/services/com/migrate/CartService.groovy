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

class CartService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService

    void migrate(Long progressId) {
        log.info("-----------------------------Starting Cart migration--------------------------------------------")
        Session hSession = sessionFactory.currentSession
        Long cartCount = hSession.createSQLQuery("SELECT count(DISTINCT entity_id) FROM sales_flat_quote").list().first() as Long
        Long timeStamp = new Date().time

        Long counter = 0
        Progress progress = Progress.findById(progressId)
        ProgressIndex progressIndex = progress.progressIndexes?.find { it.currentIndex == "CART" }
        if (progressIndex) {
            counter = progressIndex.resumePoint
            log.info("Resume migration process from index " + counter)
            log.info("Resetting processed count to " + counter)
            progressIndex.countProcessed = counter
            log.info("Updating total items count to " + cartCount)
            progressIndex.totalDocuments = cartCount
            progressIndex.save(flush: true, failOnError: true).refresh()
        } else {
            log.info("Running fresh migration for Cart")
            progressIndex = new ProgressIndex(progress: progress, currentIndex: "CART", totalDocuments: cartCount, progressStart: new Date(), status: ProgressStatus.IN_PROGRESS, countProcessed: 0, countMigrated: 0, resumePoint: 0)
            progressIndex.save(flush: true, failOnError: true).refresh()
            progress.progressIndexes.add(progressIndex)
        }
        progress.save(flush: true, failOnError: true).refresh()

        log.info(cartCount + " many carts are found to migrate")
        populateCart(progressId, counter, cartCount, timeStamp, hSession, progressIndex.id)

        progressIndex.progressEnd = new Date()
        progressIndex.status = ProgressStatus.DONE
        progressIndex.save(flush: true, failOnError: true).refresh()
        log.info("-----------------------------Ending Cart migration--------------------------------------------")
    }

    @TailRecursive
    Boolean populateCart(Long progressId, Long counter, Long cartCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping cart migration-------------------------------------------")
            MigrateJob.isInterrupted = false
            throw new MigrationException("Migration is interrupted")
        }

        if (counter < cartCount) {
            ProgressIndex progressIndex = ProgressIndex.findById(progressIndexId)

            List carts = getCartsFromDb(counter)
            log.info("Fetched " + carts.size() + " many carts from db to migrate in one iteration")
            List<Map> cartList = []

            carts.each { cart ->

                List<Map> lineItems = getLineItemsFromDb(cart["entity_id"])

                Map responseMap = [
                        id           : cart["entity_id"],
                        userId       : cart["customer_id"],
                        couponCode   : cart["coupon_code"],
                        isDeleted    : false,
                        dateCreated  : cart["created_time"],
                        lastUpdatedBy: null,
                        lastUpdated  : cart["updated_at"],
                        lineItems    : lineItems,
                ]

                progressIndex.countProcessed = progressIndex.countProcessed + 1
                progressIndex.save(flush: true, failOnError: true).refresh()

                cartList << responseMap

            }
            elasticSearchService.migrate(cartList, "cart", "cart", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            counter += 1000
            log.info("Setting resume flag at Index " + counter)
            progressIndex.resumePoint = counter
            progressIndex.save(flush: true, failOnError: true).refresh()
            populateCart(progressId, counter, cartCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getCartsFromDb(Long counter) {
        Session hSession = sessionFactory.currentSession
        SQLQuery query = hSession.createSQLQuery("SELECT entity_id,customer_id,coupon_code,created_at,updated_at" +
                " FROM sales_flat_quote LIMIT :limit,1000")
        def carts = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            list()
        }
        carts
    }

    def getLineItemsFromDb(Long quoteId) {
        Session hSession = sessionFactory.currentSession
        SQLQuery query = hSession.createSQLQuery("SELECT sfqi.product_id AS productVariationId,sfqi.qty AS quantity,cpei.value AS size FROM sales_flat_quote_item sfqi" +
                " LEFT JOIN catalog_product_entity_int cpei ON sfqi.product_id=cpei.entity_id AND cpei.attribute_id=175 " +
                "WHERE sfqi.quote_id = :quote_id")
        def lineItems = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("quote_id", quoteId)
            list()
        }
        lineItems
    }
}
