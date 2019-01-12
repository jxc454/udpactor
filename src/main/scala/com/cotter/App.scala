package com.cotter

import com.cotter.io.models.SimpleMessages.{SimpleString, SimpleInt}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

object App extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("udpactor starting...")
    ConsumerCreator.run(intToProtobuf, new ProducerCreator)
  }

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()

  def intToProtobuf(number: Int): SimpleInt = SimpleInt.newBuilder().setIntValue(number).build()
}
