package org.antipathy.sparksolr

/**
 * Properties object
 * @param inputPath The HDFS path to ingest
 * @param zooKeeperURI The ZooKeeper URI
 * @param inputFormat The input format to read
 * @param solrCollection The name of the Solr collect to push to
 * @param jaasConfLocation JAAS conf location
 * @param connectionTimeOut Connection timeout
 */
case class Properties(inputPath: String = "",
                      zooKeeperURI: String = "",
                      inputFormat: String = "",
                      solrCollection: String = "",
                      jaasConfLocation: Option[String] = None,
                      connectionTimeOut: Int = 20000)
