/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import scala.collection.JavaConversions._

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.{ SinkMode, SinkTap }
import cascading.tuple.{ Fields, TupleEntry, TupleEntryCollector }
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import kafka.utils.VerifiableProperties

object KafkaSink {
  // Default encoding is UTF-8
  val defaultSinkMode: SinkMode = SinkMode.KEEP

  def apply(brokerList: String, topic: String, sm: SinkMode = defaultSinkMode): KafkaSink =
    new KafkaSink(brokerList, topic, sm)
}

class KafkaSink(brokerList: String, topic: String, sinkMode: SinkMode)
  extends SinkTap[ProducerConfig, String] {

  private var producer: Producer[String, String] = null
  private var props: VerifiableProperties = null
  private var modifiedTime: Long = 1L

  override def createResource(conf: ProducerConfig) = {
    producer = new Producer[String, String](conf)
    props = conf.props
    true
  }

  override def deleteResource(conf: ProducerConfig) = {
    producer.close()
    producer = null
    props = null
    true
  }

  override def getIdentifier() = brokerList + "-" + topic

  override def getModifiedTime(conf: ProducerConfig) = if (resourceExists(conf)) modifiedTime else 0L

  override def openForWrite(flowProcess: FlowProcess[ProducerConfig], output: String): TupleEntryCollector = {
    new KafkaTupleEntryCollector(producer)
  }

  override def resourceExists(conf: ProducerConfig) = producer != null
}

class KafkaTupleEntryCollector(val producer: Producer[String, String]) extends TupleEntryCollector {

  override def collect(entry: TupleEntry) {
    producer.send(new KeyedMessage[String, String]("test", serialize(entry.getFields)))
  }

  // TODO - make this modular
  def serialize(fields: Fields): String = {
    fields.toString
  }
}
