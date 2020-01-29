package bigdata.streaming

import bigdata.streaming.job.StreamingJsonFilesJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery


object StructuredStreamingApp extends App {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("template")
    .getOrCreate()

  val inputDir = "/tmp/devsh-streaming"
  val streamingQuery: StreamingQuery = StreamingJsonFilesJob.runJob(inputDir)
  streamingQuery.awaitTermination()

}
