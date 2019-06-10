package com.logicalclocks.iot.spark

import io.hops.util.Hops
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object TrafficDetectionOneTopic {
  def main(args: Array[String]): Unit = {

    object Holder extends Serializable {
      @transient lazy val log = LogManager.getLogger("TrafficDetectionOneTopic")
    }

    val sparkConf: SparkConf = new SparkConf()
    val spark = SparkSession.builder.appName(Hops.getJobName).config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val log = LogManager.getLogger("TrafficDetectionOneTopic")
    log.setLevel(Level.INFO)
    log.info("Starting traffic detection job")

    val topic = "topic-lwm2m-3303-temperature"

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

    val df: DataFrame = loadDfFromKafka(topic)

    val mdf: DataFrame =
      df.select(col("key") cast "string" as 'endpointClientName,
        from_avro(col("value"), Hops.getSchema(topic)) as 'measurement).
        withColumn("timestamp", ($"measurement.timestamp"/1000).cast(TimestampType)).
        withWatermark("timestamp", "1 minute")

    val countByGatewayWindow: DataFrame = mdf.
      groupBy(window($"timestamp", "10 seconds", "5 seconds"),
        $"measurement.gatewayName").
      count().
      withColumn("windowStart", $"window.start").
      withColumn("windowEnd", $"window.end").
      drop("window")

    val detectedOverload: Dataset[Row] = countByGatewayWindow filter (_.getAs[Long]("count") > 10)

    val overloadWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true

      override def process(value: Row): Unit = {
        Hops.blockIotGateway(value.getAs[String]("gatewayName"), true)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    detectedOverload
      .writeStream
      .foreach(overloadWriter)
      .start()
      .awaitTermination()

    log.info("Shutting down TrafficDetection job")
    spark.close()

  }
}
