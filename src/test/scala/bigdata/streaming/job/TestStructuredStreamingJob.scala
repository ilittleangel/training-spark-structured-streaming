package bigdata.streaming.job

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class TestStructuredStreamingJob extends FlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  "runJob()" should "run a Structured Streaming Job" in {

    val streamingQuery = StructuredStreamingJob.runJob(spark)

    streamingQuery.awaitTermination(30 * 1000L)

    val status = spark.streams.active.map(_.status)
    status.foreach(println)
  }
}
