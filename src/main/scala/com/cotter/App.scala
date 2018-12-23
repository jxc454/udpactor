package com.cotter

import com.cotter.io.models.SimpleMessages.{SimpleString, SimpleInt}

object App {
  def main(args : Array[String]): Unit = ConsumerCreator.run(intToProtobuf, new ProducerCreator)

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()

  def intToProtobuf(number: Int): SimpleInt = SimpleInt.newBuilder().setIntValue(number).build()
}
