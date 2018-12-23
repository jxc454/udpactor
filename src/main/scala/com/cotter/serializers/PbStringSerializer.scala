package com.cotter.serializers

import java.util

import com.cotter.io.models.SimpleMessages.SimpleString
import org.apache.kafka.common.serialization.Serializer

class PbStringSerializer extends Serializer[SimpleString] {
  override def serialize(topic: String, pb: SimpleString): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}