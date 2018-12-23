package com.cotter

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, ConsumerRecord, KafkaConsumer}
import com.cotter.io.models.SimpleMessages.SimpleString

import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration
import java.util.UUID

object ConsumerCreator {
  def run(converter: String => SimpleString, producer: ProducerCreator): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "create-protobuf")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    kafkaConsumer.subscribe(Seq("incoming").asJava)

    while (true) {
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofSeconds(1))

      val recordsSeq: Seq[ConsumerRecord[String, String]] = records.iterator().asScala.toSeq

      recordsSeq.foreach(r => {
        println("producing...")
        producer.produce(converter(r.value()), UUID.randomUUID().toString)
      })
    }
  }
}
