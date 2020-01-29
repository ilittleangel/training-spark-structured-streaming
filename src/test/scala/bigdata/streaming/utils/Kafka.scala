package bigdata.streaming.utils

import java.time.Instant

import scala.util.Random

object Kafka {

  def createCardMessage(i: Int): String = {
    val cardTypes = Seq("Finanzas", "Seguros", "Salud", "Logistica")
    val cardType = cardTypes(Random.nextInt(cardTypes.size))
    val timestamp = Instant.now().getEpochSecond
    val amount = Random.nextInt(40000)
    s"""
       |{
       |  "tx_id": $i,
       |  "tx_card_type": $cardType,
       |  "tx_amount": $amount,
       |  "tx_datetime": $timestamp
       |}
       |""".stripMargin
  }

}
