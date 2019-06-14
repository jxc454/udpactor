package com.jxc454.udpactor

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import com.cotter.io.models.SimpleMessages.{SimpleInt, SimpleString}
import com.jxc454.udpactor.serializers.{PbIntDeserializer, PbIntSerializer, PbStringDeserializer, PbStringSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.logging.log4j.scala.Logging

object App extends Logging {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  def main(args: Array[String]): Unit = {
    logger.info("udpactor starting...")

    val config: Config = ConfigFactory.parseResources("streams.conf")

    logger.info(config)

    implicit val pbSimpleIntSerde: Serde[SimpleInt] = Serdes.serdeFrom(new PbIntSerializer, new PbIntDeserializer)
    implicit val pbSimpleStringSerde: Serde[SimpleString] = Serdes.serdeFrom(new PbStringSerializer, new PbStringDeserializer)

    val settings: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("applicationId"))
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrapServers"))
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()
    val numbers: KStream[String, java.lang.Integer] = builder.stream[String, java.lang.Integer](config.getString("inputTopic"))
    val pbNumbers: KStream[String, SimpleInt] = numbers.map((k: String, v: Integer) => (k, intToProtobuf(v.toInt)))

    pbNumbers.to(config.getString("outputTopic"))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), settings)
    streams.start()

    sys.ShutdownHookThread {
      streams.close()
    }
  }

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()

  def intToProtobuf(number: Int): SimpleInt = SimpleInt.newBuilder().setIntValue(number).build()
}