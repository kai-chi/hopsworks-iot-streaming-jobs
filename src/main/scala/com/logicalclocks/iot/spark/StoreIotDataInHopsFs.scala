package com.logicalclocks.iot.spark

import io.hops.util.Hops
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.col

object StoreIotDataInHopsFs extends App {

  override def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(getClass)
    log.setLevel(Level.INFO)
    log.info("Starting storing IoT data to Hops-FS")

    val sparkConf: SparkConf = new SparkConf()
    val spark = SparkSession.builder.appName(Hops.getJobName).config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    def loadDfFromKafka(topic: String): DataFrame = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers", Hops.getBrokerEndpoints).
      option("subscribe", topic).
      option("startingOffsets", "earliest").
      option("kafka.security.protocol", "SSL").
      option("kafka.ssl.truststore.location", Hops.getTrustStore).
      option("kafka.ssl.truststore.password", Hops.getKeystorePwd).
      option("kafka.ssl.keystore.location", Hops.getKeyStore).
      option("kafka.ssl.keystore.password", Hops.getKeystorePwd).
      option("kafka.ssl.key.password", Hops.getKeystorePwd).
      load()

    val topics = Array("topic-lwm2m-3302-presence", "topic-lwm2m-3303-temperature")

    val dfs: Map[String, DataFrame] = topics.map(t => (t,loadDfFromKafka(t))).toMap

    val measurementsDFs: Map[String, DataFrame] =
      dfs.map { case (t,df) =>
        (t,
          df.select(col("key") cast "string" as 'endpointClientName,
            from_avro(col("value"), Hops.getSchema(t)) as 'measurement)
        )
      }

    measurementsDFs foreach { case (t, df) => df.writeStream.format("parquet").
      option("path", "/Projects/" + Hops.getProjectName + "/Resources/iot/data/" + t).
      option("checkpointLocation", "/Projects/" + Hops.getProjectName + "/Resources/iot/checkpoint/" + t).
      partitionBy("endpointClientName").
      start()
    }

    spark.streams.awaitAnyTermination()
    log.info("Shutting down StoreIotDataInHopsFs job")
    spark.close()

  }
}
