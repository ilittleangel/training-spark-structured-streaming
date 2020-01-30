package bigdata.streaming.utils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Common {

  def withConcurrency[T](block: () => T): Future[T] = {
    Future {
      block()
    }
  }

}
