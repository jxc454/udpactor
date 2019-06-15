package com.jxc454.udpactor

import java.util.Properties
import java.util.UUID

import com.jxc454.models.SimpleMessages.{SimpleInt, SimpleString}
import com.jxc454.udpactor.serializers.{PbIntDeserializer, PbIntSerializer, PbStringDeserializer, PbStringSerializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.logging.log4j.scala.Logging

object App extends Logging {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  def main(args: Array[String]): Unit = {
    logger.info("udpactor starting...")

    val config: Config = ConfigFactory.parseResources("streams.conf")

    logger.info(config)

    // KStream uses implicit serializers
    implicit val pbSimpleIntSerde: Serde[SimpleInt] = Serdes.serdeFrom(new PbIntSerializer, new PbIntDeserializer)
    implicit val pbSimpleStringSerde: Serde[SimpleString] = Serdes.serdeFrom(new PbStringSerializer, new PbStringDeserializer)

    // KStream config
    val settings: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("applicationId"))
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrapServers"))
      p
    }

    // define input KStream
    val builder: StreamsBuilder = new StreamsBuilder()
    val numbers: KStream[String, java.lang.Integer] = builder.stream[String, java.lang.Integer](config.getString("inputTopic"))

    // define output KStream
    val pbNumbers: KStream[String, SimpleInt] = numbers.map((k: String, v: Integer) => {
      logger.info(s"udp-actor got a number: $v")
      (UUID.randomUUID.toString, intToProtobuf(v.toInt))
    })

    // define output KStream destination
    pbNumbers.to(config.getString("outputTopic"))

    // start
    val streams: KafkaStreams = new KafkaStreams(builder.build(), settings)
    streams.start()

    // stop
    sys.ShutdownHookThread {
      streams.close()
    }
  }

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()
  def intToProtobuf(number: Int): SimpleInt = SimpleInt.newBuilder().setIntValue(number).build()
}