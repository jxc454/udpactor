package com.cotter.serializers

import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import java.util

import com.cotter.io.models.SimpleMessages.SimpleInt
import org.apache.kafka.common.serialization.Serializer

class PbIntSerializer extends Serializer[SimpleInt] {
  override def serialize(topic: String, pb: SimpleInt): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}