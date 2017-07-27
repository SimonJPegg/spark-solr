package org.antipathy.sparksolr

import java.nio.file.Paths
import scopt.OptionParser

/**
 * Command line parser
 */
class CommandLineParser {

  private val parser: OptionParser[Properties] =
    new OptionParser[Properties]("SolrLoader") {

      head("spark-submit --class org.antipathy.sparksolr.SolrLoaderJob", "0.1")

      opt[String]("inputPath")
        .required()
        .action((i, p) => p.copy(inputPath = Paths.get(i).toString))
        .text("The HDFS path to ingest")

      opt[String]("zooKeeperURI")
        .required()
        .action((u, p) => p.copy(zooKeeperURI = u))
        .text("The ZooKeeper URI")

      opt[String]("inputFormat")
        .required()
        .action((i, p) => p.copy(inputFormat = i))
        .text(
          "The input format to read, supported formats are: csv,avro,parquet"
        )

      opt[String]("solrCollection")
        .required()
        .action((i, p) => p.copy(solrCollection = i))
        .text("The name of the Solr collect to push to")

      opt[String]("jaasConfLocation")
        .optional()
        .action((i, p) => p.copy(jaasConfLocation = Some(i)))
        .text("JAAS conf location (for kerberised clusters)")

      opt[Int]("connectionTimeOut")
        .optional()
        .action((i, p) => p.copy(connectionTimeOut = i))
        .text("Time in milliseconds to wait before failing connection attempt")
    }

  /**
   * Parse command line arguments and return a properties object.
   * @param stringArray the command line arguments
   * @return a properties object.
   */
  def parse(stringArray: Array[String]): Properties =
    parser.parse(stringArray, Properties()) match {
      case Some(properties) => properties
      case None => throw new IllegalArgumentException()
    }
}
