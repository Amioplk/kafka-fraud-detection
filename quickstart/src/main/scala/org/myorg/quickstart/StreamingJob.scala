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
    // set up the streaming execution environment

    val window = Time.of(60, TimeUnit.SECONDS)
    val outputPath = "output_clicks.txt"

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val displaysKafkaConsumer = new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties)
    val clicksKafkaConsumer = new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties)

    val displayStream
    = env.addSource(displaysKafkaConsumer)
    val clickStream
    = env.addSource(clicksKafkaConsumer)

    //val wordCounts = countWords(lines, stopWords, window)

    /*wordCounts
      .map(_.toString)
      .addSink(kafkaProducer)
    */

    clickStream.print()
    displayStream.print()

    // https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/streamfile_sink/
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
      .build()

    clickStream.addSink(sink)
    clickStream.sinkTo()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
