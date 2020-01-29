package bigdata.streaming.job

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import bigdata.streaming.utils.Files._
import org.apache.log4j.{Level, Logger}
import scala.concurrent.ExecutionContext.Implicits.global


import scala.util.{Failure, Success}

class StreamingJsonFilesJobTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org.apache").setLevel(Level.OFF)

  private val inputDir = "/tmp/devsh-streaming"
  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  override def beforeAll(): Unit = {
    removePath(inputDir)
    createPath(inputDir)
  }

  "StreamingJsonFilesJob.runJob" should "process Json files" in {

    withConcurrency { () => copyFiles(source = "/activations_stream", dest = inputDir, sleepTime = 10000) }
      .onComplete {
        case Success(_) => println("Copied!")
        case Failure(e) => e.printStackTrace()
      }

    val streamingQuery = StreamingJsonFilesJob.runJob(inputDir)
    streamingQuery.awaitTermination(30 * 1000L)

    val status = spark.streams.active.map(_.status)
    status.foreach(println)
  }
}
