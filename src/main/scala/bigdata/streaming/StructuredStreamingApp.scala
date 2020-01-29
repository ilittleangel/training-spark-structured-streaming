package bigdata.streaming

import bigdata.streaming.job.StructuredStreamingJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery


object StructuredStreamingApp extends App {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("template")
    .getOrCreate()

  val streamingQuery: StreamingQuery = StructuredStreamingJob.runJob
  streamingQuery.awaitTermination()

}
