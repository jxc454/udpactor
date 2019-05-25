package com.cotter

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import com.cotter.io.models.SimpleMessages.{SimpleInt, SimpleString}
import com.cotter.serializers.{PbIntDeserializer, PbIntSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}
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

    println(config)

    implicit val pbSimpleIntSerde: Serde[SimpleInt] = Serdes.serdeFrom(new PbIntSerializer, new PbIntDeserializer)

    val settings: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("applicationId"))
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrapServers"))
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()
    val numbers: KStream[String, String] = builder.stream[String, String](config.getString("incomingTopic"))
    val pbNumbers: KStream[String, SimpleInt] = numbers.map((k: String, v: String) => (k, intToProtobuf(v.toInt)))
    pbNumbers.to(config.getString("outputTopic"))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), settings)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }

    //    ConsumerCreator.run(intToProtobuf, new ProducerCreator)
  }

  def stringToProtobuf(s: String): SimpleString = SimpleString.newBuilder().setStringValue(s).build()

  def intToProtobuf(number: Int): SimpleInt = SimpleInt.newBuilder().setIntValue(number).build()
}