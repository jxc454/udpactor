package com.cotter

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import com.cotter.io.models.SimpleMessages.{SimpleInt, SimpleString}

import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration
import java.util.UUID

object ConsumerCreator {
  def run(converter: Int => SimpleInt, producer: ProducerCreator): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "create-protobuf")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")

    val kafkaConsumer: KafkaConsumer[String, Int] = new KafkaConsumer(props)
    kafkaConsumer.subscribe(Seq("incoming").asJava)

    while (true) {
      val records: ConsumerRecords[String, Int] = kafkaConsumer.poll(Duration.ofSeconds(1))

      val recordsSeq: Seq[ConsumerRecord[String, Int]] = records.iterator().asScala.toSeq

      recordsSeq.foreach(r => {
        println("producing...")
        producer.produce(UUID.randomUUID().toString, converter(r.value()))
      })
    }
  }
}