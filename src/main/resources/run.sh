#!/usr/bin/env bash

`which spark-submit` --master yarn-client \
--files jaas_solr.conf,user.keytab \
--class org.antipathy.sparksolr.SolrLoaderJob \
spark-solr-0.1-SNAPSHOT-jar-with-dependencies.jar \
--inputPath /some/hdfs/location/ \
--solrURI  zookeeperhost:2181/solr \
--inputFormat avro \
--jaasConfLocation jaas_solr.conf \
--solrCollection test-spark-avro \
--connectionTimeOut 30000
