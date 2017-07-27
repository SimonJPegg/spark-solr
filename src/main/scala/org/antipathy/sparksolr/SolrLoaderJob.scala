package org.antipathy.sparksolr

import com.databricks.spark.avro._
import org.antipathy.sparksolr.SolrImplicits._
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ Logging, SparkConf, SparkContext }

/**
 * Load Data from a HDFS location to a solr collection
 */
object SolrLoaderJob extends Logging {

  def main(args: Array[String]): Unit = {

    logInfo("Parsing command line options")
    val properties = new CommandLineParser().parse(args)
    implicit val sqlContext = getSQLContext
    logInfo(
      s"Getting ipnut data from ${properties.inputPath} as ${properties.inputFormat}"
    )
    getInputData(properties.inputPath, properties.inputFormat).toSolrDocumentRDD
      .foreachPartition(addSolrDocumentsToBatch(_, properties))

    SolrDocumentLoader.commitBatch(properties.zooKeeperURI,
                                   properties.solrCollection,
                                   properties.jaasConfLocation,
                                   properties.connectionTimeOut)

    logInfo("Complete!")
  }

  /**
   * Add an iterable of solr documents to the current batch
   * @param solrDocumentIterator the solr document iterator
   * @param properties the properties object
   */
  private def addSolrDocumentsToBatch(
      solrDocumentIterator: Iterator[SolrInputDocument],
      properties: Properties
  ) =
    while (solrDocumentIterator.hasNext) {
      SolrDocumentLoader.addToBatch(solrDocumentIterator.next(),
                                    properties.zooKeeperURI,
                                    properties.solrCollection,
                                    properties.jaasConfLocation,
                                    properties.connectionTimeOut)
    }

  /**
   * Get the SQLContext
   * @return a SQLContext
   */
  def getSQLContext: SQLContext = {
    val sparkConf = new SparkConf()
    val sparkContext = new SparkContext(sparkConf)
    new SQLContext(sparkContext)
  }

  /**
   * Get the imput data to read
   * @param inputPath the input path to read from
   * @param inputFormat the input format to read
   * @param sqlContext the SQLContext
   * @return
   */
  def getInputData(inputPath: String, inputFormat: String)(
      implicit sqlContext: SQLContext
  ): DataFrame =
    inputFormat match {
      case "avro" => sqlContext.read.avro(inputPath)
      case "csv" =>
        sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(inputPath)
      case "parquet" => sqlContext.read.parquet(inputPath)
    }
}
