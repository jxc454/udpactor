package com.cotter

import com.cotter.io.models.SimpleMessages.SimpleString

object App {
  
  def main(args : Array[String]): Unit = ConsumerCreator.run(stringToProtobuf, new ProducerCreator)

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()

}
