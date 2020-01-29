package bigdata.streaming.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger.ProcessingTime


object StreamingJsonFilesJob {

  def runJob(inputDir: String)(implicit spark: SparkSession): StreamingQuery = {

    // JSON format files containing device activations
    import org.apache.spark.sql.types._
    val activationsSchema = StructType( List(
      StructField("acct_num", IntegerType),
      StructField("dev_id", StringType),
      StructField("phone", StringType),
      StructField("model", StringType)))

    // read data from a set of streaming files
    val activationsDF = spark.readStream.schema(activationsSchema).json(inputDir)
    activationsDF.printSchema()

    // Read the input files write the data to the console
    // Append mode = only show new data (new batches containing new files)
    val activationsQuery = activationsDF.writeStream
      .outputMode("append")
      .option("truncate","false")
      .format("console").start()

    // write to console, complete mode (only allowed for aggregated queries)
    val activationCountDF = activationsDF.groupBy("model").count()
    val activationCountQuery = activationCountDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()


    // select the dev ID and account number for actionations of Titanic 1000 models
    val titanic1000DF = activationsDF.where("model = 'Titanic 1000'").select("dev_id","acct_num")
    titanic1000DF.printSchema

    // checkpointing must be configured before saving files
    spark.conf.set("spark.sql.streaming.checkpointLocation", "tmp/streaming-checkpoint")

    // save to a set of files.  Only append mode can be used when saving
    val titanic1000Query = titanic1000DF.writeStream
      .trigger(ProcessingTime("3 seconds"))
      .outputMode("append")
      .format("csv")
      .option("path","tmp/titanic1000/")
      .start()

    titanic1000Query
  }

}
