package com.migrate

import com.migrate.co.ProductCO
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

class ProductService {
    static transactional = false
    SessionFactory sessionFactory
    ElasticSearchService elasticSearchService
    CommonService commonService
    def messageSource

    void migrate(Long progressId) {
        Session hSession = null
        try {
            log.info("-----------------------------Starting Product migration--------------------------------------------")
            hSession = sessionFactory.openSession()
            Long productCount = hSession.createSQLQuery("SELECT count(DISTINCT entity_id) FROM catalog_product_entity WHERE type_id = 'configurable'").list().first() as Long
            Long timeStamp = new Date().time
            log.info(productCount + " many products are found to migrate")

            Long counter = 0
            Long progressIndexId = null
            SQLQuery query = hSession.createSQLQuery("""SELECT id,resume_point,count_valid,count_fail,count_iteration_valid,count_iteration_fail FROM progress_index
                WHERE progress_id =:progress_id AND index_name='PRODUCT'""")
            def progressIndexAttr = query.with {
                resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
                setLong("progress_id", progressId)
                uniqueResult()
            }

            if (progressIndexAttr) {
                counter = progressIndexAttr["resume_point"]
                progressIndexId = progressIndexAttr["id"]
                if (counter < productCount) {
                    Long countValid = progressIndexAttr["count_valid"] - progressIndexAttr["count_iteration_valid"]
                    Long countFail = progressIndexAttr["count_fail"] - progressIndexAttr["count_iteration_fail"]
                    log.info("Resume migration process from index " + counter)
                    log.info("Resetting processed count to " + counter)
                    log.info("Updating total items count to " + productCount)
                    query = hSession.createSQLQuery("""UPDATE progress_index SET count_processed =:count_processed, total_documents =:total_documents, count_valid =:count_valid,
                count_fail =:count_fail, count_iteration_valid =0, count_iteration_fail =0, is_current =true, status =:status WHERE id =:id""")
                    query.with {
                        setLong("count_processed", counter)
                        setLong("total_documents", productCount)
                        setLong("count_valid", countValid)
                        setLong("count_fail", countFail)
                        setString("status", ProgressStatus.IN_PROGRESS.toString())
                        setLong("id", progressIndexId)
                        executeUpdate()
                    }
                }

            } else {
                log.info("Running fresh migration for Products")
                query = hSession.createSQLQuery("""INSERT INTO progress_index(progress_id,index_name,total_documents,progress_start,status,count_processed,count_migrated,
                resume_point,version,is_current,count_valid,count_fail,count_iteration_valid,count_iteration_fail,bulk_error_message)
                VALUES (:progress_id,:index_name,:total_documents,:progress_start,:status,0,0,0,0,true,0,0,0,0,'')""")
                query.with {
                    setLong("progress_id", progressId)
                    setString("index_name", "PRODUCT")
                    setLong("total_documents", productCount)
                    setTimestamp("progress_start", new Date())
                    setString("status", ProgressStatus.IN_PROGRESS.toString())
                    executeUpdate()
                }

                query = hSession.createSQLQuery("SELECT id FROM progress_index WHERE progress_id =:progress_id AND index_name='PRODUCT'")
                progressIndexId = query.with {
                    setLong("progress_id", progressId)
                    uniqueResult()
                }
            }
            hSession.flush()

            if (counter < productCount) {
                populateProduct(progressId, counter, productCount, timeStamp, hSession, progressIndexId)

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

            log.info("-----------------------------Ending Product migration--------------------------------------------")
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
    Boolean populateProduct(Long progressId, Long counter, Long productCount, Long timeStamp, Session hSession, Long progressIndexId) {
        if (MigrateJob.isInterrupted) {
            log.info("--------------------------------Stopping product migration-------------------------------------------")
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

        if (counter < productCount) {

            List products = getProductsFromDb(counter, hSession)
            log.info("Fetched " + products.size() + " many products from db to migrate in one iteration")
            List<Map> productList = [].asSynchronized()
            String errorTrackingMessage = null

            def productCallable = { product, hSessionInner ->
                try {
                    List attributes = getAttributes(product["entity_id"], hSessionInner)

                    List productVariations = getProductVariationsFromDb(product["entity_id"], hSessionInner)
                    List<Map> productVariationList = []
                    List<Map> productSizeList = []
                    String colorName = null
                    String colorCode = null

                    productVariations.each { productVariation ->

                        List productVariationAttributes = getAttributes(productVariation["entity_id"], hSessionInner)

                        Map productVariationAttrMap = commonService.addAttributes([:], productVariationAttributes)

                        Map productVariationMap = [
                                id           : productVariation["sku"].toString(),
                                size         : productVariationAttrMap["size"],
                                stockQuantity: productVariation["qty"] ? productVariation["qty"] as Integer : 0,
                        ]

                        colorName = productVariationAttrMap["colorName"]
                        colorCode = productVariationAttrMap["colorCode"]
                        if (productVariationMap["stockQuantity"] && productVariationMap["stockQuantity"] > 0) {
                            productVariationList << productVariationMap
                        }

                        Map productSizeMap = [
                                id               : productVariation["sku"].toString(),
                                size             : productVariationAttrMap["size"],
                                name             : productVariationAttrMap["name"],
                                eanNumber        : productVariationAttrMap["eanNumber"],
                                messageTrackingId: null,
                                holdQuantity     : 0,
                                description      : productVariationAttrMap["description"],
                                shortDescription : productVariationAttrMap["shortDescription"],
                                compositionCode  : productVariationAttrMap["compositionCode"],
                                designCode       : productVariationAttrMap["designCode"],
                                seasonCode       : productVariationAttrMap["seasonCode"],
                        ]

                        productSizeList << productSizeMap
                    }

                    List productCategoryInfoList = getProductCategoryInfoFromDb(product["entity_id"], hSessionInner)
                    List<Map> productCategoryInformations = []
                    List<Map> customSortRuleIndexes = []

                    productCategoryInfoList.each { productCategoryInfo ->
                        //if (!productCategoryInfo["children_count"]) {
                        List productCategoryPath = CommonUtils.getPathList(productCategoryInfo["path"].substring(4))

                        Map productCategoryInfoMap = [
                                categoryId  : productCategoryInfo["category_id"].toString(),
                                categoryName: productCategoryInfo["value"],
                                categoryPath: productCategoryPath
                        ]

                        productCategoryInformations << productCategoryInfoMap

                        Map customSortRuleMap = [
                                ruleIndex     : null,
                                categoryId    : productCategoryInfo["category_id"].toString(),
                                sortValue     : null,
                                bestValueIndex: "${productCategoryInfo["position"] + MigrationConstants.BEST_VALUE_INDEX_ADD}"
                        ]

                        customSortRuleIndexes << customSortRuleMap
                        // }
                    }

                    int brokenSizes = 0
                    if (productSizeList.size() > productVariationList.size()) {
                        brokenSizes = productVariationList.size()
                    }

                    Map responseMap = [
                            id                         : product["entity_id"].toString(),
                            name                       : null,
                            sku                        : product["sku"],
                            color                      : null,
                            colorCode                  : null,
                            colorName                  : null,
                            description                : null,
                            brand                      : null,
                            discount                   : 0.0,
                            asStylingTips              : null,
                            infoAndCare                : null,
                            styleCode                  : null,
                            premiumPackagingSKU        : null,
                            taxClass                   : null,
                            shortDescription           : null,
                            coverage                   : null,
                            department                 : null,
                            gender                     : null,
                            fabric                     : null,
                            isActive                   : null,
                            qcStatus                   : null,
                            price                      : null,
                            weight                     : null,
                            productVariations          : productVariationList,
                            yearOfManufacture          : null,
                            activationDate             : null,
                            productCategoryInformations: productCategoryInformations,
                            imageData                  : [],
                            sizeChart                  : null,
                            brokenSizes                : brokenSizes, //change
                            customSortRuleIndexes      : customSortRuleIndexes,
                            productSizes               : productSizeList,
                            applicableDiscounts        : [],
                            positionPerCategory        : [],
                            currentDiscount            : null,
                            soldOutDate                : null,
                            liveDate                   : null,
                            qcPassedDate               : null,
                            qtyUpdatedDate             : null,
                            modelSizeDescription       : null,
                            qcPassedBy                 : null,
                            volumetric_weight          : null,
                            isLive                     : null,
                            baseCategory               : null,
                            urlKey                     : null,
                            urlPath                    : null,
                            isDeleted                  : false,
                            dateCreated                : product["created_at"] ? product["created_at"].time : timeStamp,
                            lastUpdated                : product["updated_at"] ? product["updated_at"].time : timeStamp,
                            lastUpdatedBy              : "",

                    ]

                    responseMap = commonService.addAttributes(responseMap, attributes)
                    responseMap["activationDate"] = responseMap["isActive"] ? (responseMap["liveDate"] ?: timeStamp) : null
                    responseMap["isLive"] = responseMap["liveDate"] ? "YES" : "NO"
                    responseMap["qcStatus"] = responseMap["isActive"]
                    responseMap["qcPassedDate"] = responseMap["qcStatus"] ? new Date().time : null
                    List additionalInputs = [responseMap["brand"], responseMap["color"]]?.grep()
                    responseMap["suggest"] = CommonUtils.createSuggestions(responseMap["name"], responseMap["name"], responseMap["sku"], 0, additionalInputs)
                    responseMap["colorName"] = colorName
                    responseMap["colorCode"] = colorCode

                    if (productVariationList.size() == 0) {
                        if (responseMap["liveDate"] || responseMap["isActive"]) {
                            responseMap["soldOutDate"] = (new Date() - 5).time
                        }

                        if (!responseMap["liveDate"] && responseMap["isActive"]) {
                            responseMap["liveDate"] = (new Date() - 6).time
                            responseMap["isLive"] = "YES"
                        }
                    } else {
                        if (!responseMap["liveDate"] && responseMap["isActive"]) {
                            responseMap["liveDate"] = new Date().time
                            responseMap["isLive"] = "YES"
                        }
                    }

                    Map currentDiscount = [
                            discountedAmount         : responseMap["price"],
                            discountValue            : 0,
                            discountValueInPercentage: 0,
                            stopFurtherRuleProcessing: null,
                            isDiscountInPercent      : null,
                            discountId               : null
                    ]
                    responseMap["currentDiscount"] = currentDiscount

                    responseMap = postProcessing(responseMap)

                    Map errorMap = validateProduct(responseMap)
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

                    productList << responseMap
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
            products.each { product ->
                Session hSessionInner = sessionFactory.openSession()
                pool.execute({ productCallable(product, hSessionInner) } as Callable)
            }
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)

            if (errorTrackingMessage) {
                throw new MigrationException("Worker threads are unable to process due to some reason")
            }

            elasticSearchService.migrate(productList, "product", "product", progressId, hSession, progressIndexId)
            log.info("Starting next iteration")
            Long resumePoint = counter + products.size()
            counter += MigrationConstants.PRODUCT_ITERATION_SIZE
            log.info("Setting resume flag at Index " + resumePoint)

            SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET resume_point =:resume_point, count_iteration_valid=0, count_iteration_fail=0 WHERE id =:id""")
            query.with {
                setLong("id", progressIndexId)
                setLong("resume_point", resumePoint)
                executeUpdate()
            }
            hSession.flush()

            populateProduct(progressId, counter, productCount, timeStamp, hSession, progressIndexId)
        } else {
            true
        }
    }

    def getProductsFromDb(Long counter, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT entity_id, created_at, updated_at, sku, type_id FROM catalog_product_entity WHERE type_id = 'configurable' LIMIT :limit,:totalSize")
        def products = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("limit", counter)
            setLong("totalSize", MigrationConstants.PRODUCT_ITERATION_SIZE)
            list()
        }
        products
    }

    def getProductVariationsFromDb(Integer parentId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("SELECT cpe.entity_id, cpe.created_at, cpe.updated_at, cpe.sku, cpe.type_id, csi.qty FROM catalog_product_relation cpr, catalog_product_entity cpe, cataloginventory_stock_item csi WHERE cpr.child_id = cpe.entity_id AND csi.product_id=cpr.child_id AND cpr.parent_id = :parent_id")
        def simpleProducts = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("parent_id", parentId)
            list()
        }
        simpleProducts
    }

    def getProductCategoryInfoFromDb(Integer productId, Session hSession) {

        SQLQuery query = hSession.createSQLQuery("""SELECT ccp.category_id ,ccp.position ,cce.path, ccev.value, cce.children_count
        FROM catalog_category_product ccp LEFT JOIN catalog_category_entity cce ON cce.entity_id = ccp.category_id
        LEFT JOIN catalog_category_entity_varchar ccev ON ccev.entity_id = cce.entity_id AND ccev.attribute_id = 41
        WHERE ccp.product_id = :product_id and cce.children_count=0 and ccp.category_id NOT IN (344,345,178,521,501,349,348,179,531,511) order by ccp.category_id""")
        def productCategoryInfo = query.with {
            resultTransformer = AliasToEntityMapResultTransformer.INSTANCE
            setLong("product_id", productId)
            list()
        }
        productCategoryInfo
    }

    List getAttributes(Integer id, Session hSession) {

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

    Map postProcessing(Map responseMap) {
        if (!responseMap["baseCategory"] && responseMap["productCategoryInformations"]) {
            /*List categoryPathList = responseMap["productCategoryInformations"]?.sort {
                it.categoryId.toInteger()
            }?.first()?.categoryPath*/
            List categoryPathList = responseMap["productCategoryInformations"]?.first()?.categoryPath
            if (categoryPathList) {
                int size = categoryPathList.size()
                responseMap["baseCategory"] = categoryPathList[size - 1]
            }

        }
        responseMap
    }

    Map validateProduct(Map product) {
        Map errorMap = null
        ProductCO co = new ProductCO()
        DataBindingUtils.bindObjectToInstance(co, product, ["productCategoryInformations", "sku", "name", "price", "taxClass", "gender"], [], null)
        if (!co.validate()) {
            //product["isDeleted"] = true
            product["isActive"] = false
            String message = co.errors.allErrors.collect {
                messageSource.getMessage(it, null)
            }.join("<br/>")
            log.error "Validation failed for product : [${co.sku} | ${co.name}], message : ${message}"
            errorMap = [
                    entityId     : product.id,
                    failureReason: message,
            ]
        }
        return errorMap
    }
}
