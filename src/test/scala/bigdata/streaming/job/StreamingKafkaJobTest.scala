package bigdata.streaming.job

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import bigdata.streaming.utils.Kafka._
import org.apache.log4j.{Level, Logger}


class StreamingKafkaJobTest extends FlatSpec with Matchers with EmbeddedKafka {

  Logger.getLogger("org").setLevel(Level.OFF)

  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  val inputTopic = "events"
  val outputTopic = "notifications"
  val consumerGroup = "groupId"
  val kafkaPort = 6001
  private val testConsumerConfig = Map(ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup)
  private implicit val embeddedConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = 6000, customConsumerProperties = testConsumerConfig)

  "StreamingKafkaJob.runJob" should "consume Kafka message" in {

    withRunningKafka {
      (1 to 10).toList.foreach { i => publishStringMessageToKafka(inputTopic, createCardMessage(i)) }

      val streamingQuery = StreamingKafkaJob.runJob(s"localhost:$kafkaPort", inputTopic, outputTopic)
      streamingQuery.awaitTermination(30 * 1000L)

      val status = spark.streams.active.map(_.status)
      status.foreach(println)
    }
  }
}
