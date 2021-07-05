/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {

  def main(args: Array[String]): Unit = {

    // Parameters
    val window = Time.of(60, TimeUnit.SECONDS)
    val outputPath = "/Users/amirworms/Documents/Streaming/kafka-fraud-detection/quickstart/snapshots"

    val outputFile = "output_clicks.txt"

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    // Consumers
    val displaysKafkaConsumer = new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties) // Possible to use KafkaDeserializationSchema
    val clicksKafkaConsumer = new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties)

    val displayStream
    = env.addSource(displaysKafkaConsumer)
    val clickStream
    = env.addSource(clicksKafkaConsumer)

    // Print
    clickStream.print()
    displayStream.print()

    /* Sink to Kafka producer
    // 1st idea : https://sparkbyexamples.com/kafka/apache-kafka-consumer-producer-in-scala/?fbclid=IwAR2F8hs69ka7JjjMdqeVx5p35sVf66jQoQqnp-qLft998NbR9lGMl4TntkU
    // 2nd idea : https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/
    val properties_producer = new Properties
    properties_producer.setProperty("bootstrap.servers", "localhost:9092")

    val topic = "fraud_clicks"
    val fraudClicksProducer = new FlinkKafkaProducer[String](
      topic,                  // target topic
      new KafkaSerializationSchema[String]() {
        override def serialize(element: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8))
      },    // serialization schema #https://stackoverflow.com/questions/58963483/scala-string-sink-for-flinkkafkaproducer-extending-kafkaserializationschema
      properties_producer,                  // producer config
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE) // fault-tolerance

    clickStream.addSink(fraudClicksProducer)

    //fraudClicksProducer.send(record)

    "Error while fetching metadata with correlation id"
    -> Test that the "kafka" and "mongo" domain names resolve to the correct brokers (e.g. with kafka-manager ?), and that the port is open to the machine that the monitor is running on.

    */
    // https://www.codota.com/code/java/classes/org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
    import org.apache.kafka.clients.consumer.ConsumerRecord
    import org.apache.kafka.clients.consumer.ConsumerRecords
    import java.time.Duration
    while ( {
      true
    }) {
      val records = clicksKafkaConsumer.poll(Duration.ofMillis(100)) // Fetcher has to be used instead : https://ci.apache.org/projects/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/connectors/kafka/FlinkKafkaConsumer.html
      import scala.collection.JavaConversions._
      for (record <- records) {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset, record.key, record.value)
      }
    }

    // Sink to a file
    // https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/streamfile_sink/
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .build()

    clickStream.addSink(sink).name("Fraudulent Clicks Sink")

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
