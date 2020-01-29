package bigdata.streaming.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery, Trigger}

object StreamingKafkaCompleteJob {

  def runJob(implicit spark: SparkSession): StreamingQuery = {

    import spark.implicits._

    //definimos el nº de particiones para las agregaciones(por defecto=200)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    val topic: String = "activations"

    //creamos el readStream para la conexion a Kafka
    val kafkaDF = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers", "quickstart.cloudera:9092").
      option("subscribe", topic).load()

    //declaramos un esquema para los datos recibidos en formato JSON
    val esquema = StructType(List(
      StructField("acct_num", IntegerType),
      StructField("dev_id", StringType),
      StructField("phone", StringType),
      StructField("model", StringType)
    ))

    val activationsDF = kafkaDF.select(from_json($"value".cast("string"), esquema).
      alias("activation"))

    //contamos las filas en el activationsDF ordenadas en modo descendente por modelo
    val sortedModelCountDF = activationsDF.groupBy($"activation"("model")).count()

    //iniciamos una query basada en sortedModelCountDF , que nos mostrara los resultados
    // "completos" y establecera un trigger de 5 segundos.
    val sortedModelCountQuery = sortedModelCountDF.writeStream.
      outputMode("complete").
      format("console"). //ojo, que se me ha olvidado
      option("truncate", "false").
      //trigger(ProcessingTime("5 segundos")).//esta deprecated, buscar alternativa
      trigger(Trigger.ProcessingTime(5000)).
      start()

    sortedModelCountQuery
  }

}
