package org.antipathy.sparksolr

import org.apache.solr.client.solrj.impl._
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.Logging
import scala.collection.mutable

/**
 * Loads Solr documents into a collection
 */
object SolrDocumentLoader extends Logging with Serializable {

  private val cachedConnections = mutable.Map[String, CloudSolrServer]()

  /**
   * Add a solr document to the current batch
   * @param document the document to add
   * @param serverURI the URI of the ZooKeeper server
   * @param solrCollection the solr collection name
   * @param jaasConf an optional JAAS conf file location
   * @param timeout inital connection timeout
   * @return an UpdateResponse object
   */
  def addToBatch(document: SolrInputDocument,
                 serverURI: String,
                 solrCollection: String,
                 jaasConf: Option[String],
                 timeout: Int): UpdateResponse =
    getConnection(serverURI, solrCollection, jaasConf, timeout).add(document)

  /**
   * Commit the current batch to the solr collection
   * @param serverURI the URI of the ZooKeeper server
   * @param solrCollection the solr collection name
   * @param jaasConf an optional JAAS conf file location
   * @param timeout inital connection timeout
   * @return an UpdateResponse object
   */
  def commitBatch(serverURI: String,
                  solrCollection: String,
                  jaasConf: Option[String],
                  timeout: Int): UpdateResponse =
    getConnection(serverURI, solrCollection, jaasConf, timeout).commit()

  /**
   * Returns a connection to a solr collection
   * @param serverURI the URI of the ZooKeeper server
   * @param solrCollection the solr collection name
   * @param jaasConf an optional JAAS conf file location
   * @param timeout inital connection timeout
   * @return a solr connection
   */
  private def getConnection(serverURI: String,
                            solrCollection: String,
                            jaasConf: Option[String],
                            timeout: Int): CloudSolrServer =
    cachedConnections match {
      case _ if !cachedConnections.contains(solrCollection) =>
        jaasConf match {
          case Some(authenticate) =>
            setAuthentication(authenticate)
          case None =>
        }

        logInfo(s"Connecting to : $serverURI")
        val connection = new CloudSolrServer(serverURI)
        connection.setDefaultCollection(solrCollection)
        connection.setZkConnectTimeout(timeout)
        connection.connect()
        logInfo("Connected!")
        cachedConnections += (solrCollection -> connection)
        connection
      case _ if cachedConnections contains solrCollection =>
        cachedConnections(solrCollection)
    }

  /**
   * Set kerberos config
   * @param jaasConf the JAAS conf location
   */
  private def setAuthentication(jaasConf: String) = {
    logInfo("Kerberos config detected, setting up")
    System.setProperty("java.security.auth.login.config", jaasConf)
    HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer())
  }

}
