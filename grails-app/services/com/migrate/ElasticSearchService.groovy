package com.migrate

import com.migrate.enums.ProgressStatus
import com.migrate.exception.MigrationException
import grails.async.Promise
import grails.async.Promises
import grails.util.Holders
import migration.Progress
import migration.ProgressIndex
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.NodeDisconnectedException
import org.hibernate.SQLQuery
import org.hibernate.Session

class ElasticSearchService {
    TransportClient client
    private static final Object clientLock = new Object();

    void migrate(List<Map> objectList, String index, String type, Long progressId, Session hSession, Long progressIndexId, Boolean updateProgress = true) {

        try {
            synchronized (clientLock)
            {
                if (!client) {
                    log.info("Found flag value : "+ Holders.config.elasticsearch.isHostedOnFound as Boolean)
                    if (Holders.config.elasticsearch.isHostedOnFound as Boolean) {

                        log.info("In Found Elastic search")
                        String clusterName = Holders.config.elasticsearch.clusterName as String
                        String username = Holders.config.elasticsearch.username
                        String password = Holders.config.elasticsearch.password

                        InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(
                                Holders.config.elasticsearch.transportClientIP as String,
                                Holders.config.elasticsearch.transportClientPort as Integer
                        )

                        // Build the settings for our client.
                        Settings settings = ImmutableSettings.settingsBuilder()
                                .put("transport.ping_schedule", "5s")
                                .put("transport.sniff", false) //changed to check No Node Exception
                                .put("cluster.name", clusterName)
                                .put("action.bulk.compress", false)
                                .put("shield.transport.ssl", true)
                                .put("request.headers.X-Found-Cluster", clusterName)
                                .put("shield.user", "${username}:${password}")
                                .put("transport.tcp.connect_timeout", "600s")
                                .put("client.transport.ping_timeout", "1200s")
                                .build();

                        // Instantiate a TransportClient and add the cluster to the list of addresses to connect to.
                        // Only port 9343 (SSL-encrypted) is currently supported.
                        client = new TransportClient(settings)
                                .addTransportAddress(inetSocketTransportAddress);
                    } else {
                        log.info("In simple Elastic search")
                        Settings settings = ImmutableSettings.settingsBuilder()
                                .put("cluster.name", Holders.config.elasticsearch.clusterName as String).build();
                        client = new TransportClient(settings)
                                .addTransportAddress(new InetSocketTransportAddress(
                                Holders.config.elasticsearch.transportClientIP as String,
                                Holders.config.elasticsearch.transportClientPort as Integer
                        ))
                    }
                    log.info("Cluster: [${Holders.config.elasticsearch.clusterName as String}]")
                    log.info("Client: [${Holders.config.elasticsearch.transportClientIP}]")
                    log.info("Port: [${Holders.config.elasticsearch.transportClientPort}]")

                }
            }

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            objectList.each { object ->
                bulkRequest.add(client.prepareIndex(index, type, object["id"].toString()).setSource(object))
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet()

            if (bulkResponse.hasFailures()) {
                StringBuilder builder = new StringBuilder()
                log.info("----------------------------------------------------Listing Failures Start-------------------------------------")
                bulkResponse.findAll { it.failed }?.grep()?.each {
                    String message = "index : [${it.index}], type: [${it.type}], id: [${it.id}], message: [${it.failureMessage}]"
                    log.error("index : [${it.index}], type: [${it.type}], id: [${it.id}], message: [${it.failureMessage}]")
                    builder.append(message)
                    builder.append("\n")
                }
                log.info("----------------------------------------------------Listing Failures End-------------------------------------")

                SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET bulk_error_message=:error_message, progress_end =:progress_end, status =:status WHERE id =:id""")
                query.with {
                    setTimestamp("progress_end", new Date())
                    setString("status", ProgressStatus.ERROR.toString())
                    setString("error_message", builder.toString())
                    setLong("id", progressIndexId)
                    executeUpdate()
                }
                hSession.flush()
                hSession.close()

                throw new ElasticsearchException("Bulk index failure")
            } else {
                if (updateProgress) {
                    def count = bulkResponse.findAll { !it.failed }?.grep()?.size()

                    SQLQuery query = hSession.createSQLQuery("""UPDATE progress_index
                SET count_migrated = count_migrated + :num WHERE id =:id""")
                    query.with {
                        setLong("id", progressIndexId)
                        setLong("num", count)
                        executeUpdate()
                    }
                    hSession.flush()
                }
            }
        }catch (NoNodeAvailableException e) {
            log.error(e.message, e)
            log.info("Connected Nodes Size " + client.connectedNodes().size())
            if (MigrateJob.isInterrupted) {
                log.info("--------------------------------Stopping elasticsearch migration-------------------------------------------")
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
            /*while (client.connectedNodes().size() < 1)
            {
                log.info("No Node connected. Sleeping for 5 seconds. Connected node size is : "+client.connectedNodes().size())
                Thread.sleep(5000)
            }*/
            log.info("Sleeping for 10 seconds")
            Thread.sleep(10000)
            log.info("Calling migration again")
            migrate(objectList, index, type,progressId,hSession,progressIndexId,updateProgress)
        }
        catch (Exception e){
            log.error(e.message,e)
            throw e
        }

    }
}
