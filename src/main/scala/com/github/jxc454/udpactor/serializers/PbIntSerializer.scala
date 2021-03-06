package com.github.jxc454.udpactor.serializers

import java.util

import com.github.jxc454.models.SimpleMessages.SimpleInt
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class PbIntSerializer extends Serializer[SimpleInt] {
  override def serialize(topic: String, pb: SimpleInt): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

class PbIntDeserializer extends Deserializer[SimpleInt] {
  override def deserialize(topic: String, bytes: Array[Byte]): SimpleInt = SimpleInt.parseFrom(bytes)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}
