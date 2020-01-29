package bigdata.streaming.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._


object StreamingKafkaJob {

  def runJob(kafkaBootstrapServers: String, inputTopic: String, outputTopic: String)
            (implicit spark: SparkSession): StreamingQuery = {

    val tx_detail_df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .load()

    tx_detail_df.printSchema()

    val detaildf1 = tx_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    // Define a schema for the tx_detail data
    val tx_detail_schema = StructType(Array(
      StructField("tx_id", StringType),
      StructField("tx_card_type", StringType),
      StructField("tx_amount", StringType),
      StructField("tx_datetime", StringType)
    ))

    val tx_detail_df2 = detaildf1
      .select(from_json(col("value"), tx_detail_schema)
        .as("tx_detail"), col("timestamp"))

    val tx_detail_df3 = tx_detail_df2.select("tx_detail.*", "timestamp")

    // Simple aggregate - find total_tx_amount by grouping tx_card_type
    val tx_detail_df4 = tx_detail_df3
      .groupBy("tx_card_type")
      .agg(sum(col("tx_amount")).as("total_tx_amount"))

    tx_detail_df4.printSchema()

    val tx_detail_df5 = tx_detail_df4.withColumn("key", lit(100))
      .withColumn("value", concat(lit("{'tx_card_type': '"),
        col("tx_card_type"),
        lit("', 'total_tx_amount: '"),
        col("total_tx_amount").cast("string"), lit("'}")))

    tx_detail_df5.printSchema()

    // Write final result into console for debugging purpose
    val trans_detail_ws = tx_detail_df5.writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))

      // OutputMode in which only the rows that were updated in the streaming DF/DS will be written to the
      // sink every time there are some updates.
      .outputMode("update") //"complete" y "append"
      .option("truncate", "false")
      .format("console")
      .start()

    // Write final result in the Kafka topic as key, value
    val trans_detail_ws_1 = tx_detail_df5.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("checkpointLocation", "/tmp/checkpoint1") // borrar en cada ejecucion
      .start()

    trans_detail_ws_1
  }

}
