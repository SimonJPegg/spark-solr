package org.antipathy.sparksolr

import org.apache.solr.common.SolrInputDocument
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Solr implicits
 */
object SolrImplicits extends Serializable with Logging {

  /**
   * Implicit functions for DataFrames
   * @param dataFrame the `DataFrame` to operate on
   */
  implicit class DataFrameExtensions(dataFrame: DataFrame) {

    /**
     * Convert the current `DataFrame` to an RDD of SolrDocuments
     * @return an RDD of SolrDocuments
     */
    def toSolrDocumentRDD: RDD[SolrInputDocument] = {
      val fields = dataFrame.columns
      dataFrame.map { row =>
        val document = new SolrInputDocument()
        logDebug("Created new Solr Document")
        for (fieldIterator <- 0 until row.length) {
          logDebug(
            s"Added field: ${fields(fieldIterator)} with value: ${row.get(fieldIterator)}"
          )
          document.addField(fields(fieldIterator), row.get(fieldIterator))
        }
        document
      }
    }
  }
}
