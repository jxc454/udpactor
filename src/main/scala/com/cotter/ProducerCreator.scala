package com.cotter

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.cotter.io.models.SimpleMessages.SimpleInt
import java.util.Properties

class ProducerCreator {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.cotter.serializers.PbIntSerializer")

  val producer = new KafkaProducer[String, SimpleInt](props)

  def produce(key: String, pb: SimpleInt): Unit = {
    val data = new ProducerRecord("protobuf", key, pb)
    producer.send(data)
  }
}