package bigdata.streaming.job

import bigdata.streaming.utils.Common._
import bigdata.streaming.utils.Kafka._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ser.std.StringSerializer
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


class StreamingKafkaJobTest extends FlatSpec with Matchers with EmbeddedKafka {

  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  val inputTopic = "events"
  val outputTopic = "notifications"
  val consumerGroup = "groupId"
  val kafkaPort = 6001
  private val testConsumerConfig = Map(ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup)
  private implicit val embeddedConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = 6000, customConsumerProperties = testConsumerConfig)

  private implicit val serializer: StringSerializer = new StringSerializer
  private implicit val deserializer: StringDeserializer = new StringDeserializer

  "StreamingKafkaJob.runJob" should "consume Kafka message" in {

    withRunningKafka {

      withConcurrency { () => (1 to 100).foreach { i => publishStringMessageToKafka(inputTopic, createCardMessage(i)) } }
        .onComplete {
          case Success(_) => println("Published!")
          case Failure(e) => e.printStackTrace()
        }

      withConcurrency { () => consumeNumberMessagesFrom(inputTopic, 9).map(message => s"Consumed message: $message").foreach(println) }
        .onComplete {
          case Success(_) => println("Consumed!")
          case Failure(e) => e.printStackTrace()
        }

      val streamingQuery = StreamingKafkaJob.runJob(s"localhost:$kafkaPort", inputTopic, outputTopic)
      streamingQuery.awaitTermination(60 * 1000L)

      val status = spark.streams.active.map(_.status)
      status.foreach(println)

    }
  }

}
