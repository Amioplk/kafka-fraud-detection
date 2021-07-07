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

package flinkiasd;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Int;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Docs
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		// Global parameters
		Path outputPath = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/all");
		Path outputPathPattern1 = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/snapshots");

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");

		List<String> topics = new ArrayList<>();
		topics.add("clicks");
		topics.add("displays");

		// Connect with Kafka and add Watermarks
		DataStream<Event> events = env.addSource(new FlinkKafkaConsumer<>(topics, new DeserializationToEventSchema(), properties)).setParallelism(1);
		DataStream<Event> datastream = events.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Event>() {

			@Override
			public long extractTimestamp(Event event, long l) {
				return event.getTimestamp();
			}

			@Nullable
			@Override
			public Watermark checkAndGetNextWatermark(Event event, long l) {
				return new Watermark(l);
			}
		});

		/*
		//Fraud detector of duplicate UIDs which clicks too often
		FraudDetectorUid detectorUid = new FraudDetectorUid();
		//Fraud detector of IP which clicks too often
		FraudDetectorIp detectorIp = new FraudDetectorIp();

		//Take as input the Event Stream (click or display) and return an Alert Stream of fraudulent UIDs
		DataStream<UserId> alertsUid = events
				.keyBy(Event::getUid)
				.process(detectorUid)
				.name("fraud-detector-uid")
				.setParallelism(1);

		//Take as input the Event Stream (click or display) and return an Alert Stream of fraudulent IPs
		DataStream<IpAdress> alertsIp = events
				.keyBy(Event::getIp)
				.process(detectorIp)
				.name("fraud-detector-ip")
				.setParallelism(1);
		*/

		//Fraud detector of clicks that have no corresponding display
		ClickWithoutDisplayDetector detectorClickWithoutDisplay = new ClickWithoutDisplayDetector();

		// Intial Click rate
		SingleOutputStreamOperator<Tuple2<String, Integer>> clickRate = events.map(new MapFunction<Event, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Event event) throws Exception {
				return new Tuple2<String, Integer>(event.getEventType(), 1);
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple2<String, Integer>(x.f0,  x.f1 + y.f1));

		// Pattern 1 - Clicks without display
		DataStream<Event> streamClickWithoutDisplay = datastream
				.keyBy(Event::getImpressionId)
				.process(detectorClickWithoutDisplay)
				.name("fraud-detector-impressionId")
				.setParallelism(1);

		// Create sinks
		final StreamingFileSink<Event> sinkPattern1 = StreamingFileSink
				.forRowFormat(outputPathPattern1, new SimpleStringEncoder<Event>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();
		final StreamingFileSink<Event> sink = StreamingFileSink
				.forRowFormat(outputPath, new SimpleStringEncoder<Event>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();

		SingleOutputStreamOperator<Tuple2<String, Integer>> clickRatePattern1 = streamClickWithoutDisplay.map(new MapFunction<Event, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Event event) throws Exception {
				return new Tuple2<String, Integer>(event.getEventType(), 1);
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple2<String, Integer>(x.f0,  x.f1 + y.f1));

		// Add sinks
		//events.addSink(sink);
		//streamClickWithoutDisplay.addSink(sinkPattern1);

		//alertsUid.print();
		//alertsIp.print();
		//streamClickWithoutDisplay.print();
		clickRate.print();
		clickRatePattern1.print();

		// Execute
		env.execute("Flink Streaming Java API to detect fraudulent clicks in ads.");
	}
}
