package com.github.jxc454.udpactor.serializers

import java.util

import com.github.jxc454.models.SimpleMessages.SimpleString
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class PbStringSerializer extends Serializer[SimpleString] {
  override def serialize(topic: String, pb: SimpleString): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

class PbStringDeserializer extends Deserializer[SimpleString] {
  override def deserialize(topic: String, bytes: Array[Byte]): SimpleString = SimpleString.parseFrom(bytes)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}