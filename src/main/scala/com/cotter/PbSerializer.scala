package com.cotter

import java.util

import org.apache.kafka.common.serialization.Serializer
import com.cotter.io.models.SimpleMessages.SimpleString

class PbSerializer extends Serializer[SimpleString] {
  override def serialize(topic: String, pb: SimpleString): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}
