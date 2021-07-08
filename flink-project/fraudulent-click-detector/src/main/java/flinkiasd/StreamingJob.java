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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Main class to run that reads from a Kafka stream and finds 3 different patterns of suspicious clicks
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		// Global parameters
		Path outputPath = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/all");
		Path outputPathPattern1 = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/snapshots_pattern_1");
		Path outputPathPattern2 = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/snapshots_pattern_2");
		Path outputPathPattern3 = new Path("/Users/amirworms/Documents/Streaming/kafka-fraud-detection/flink-project/fraudulent-click-detector/outputs/snapshots_pattern_3");

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

		// Set properties
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

		// Fraud detectors
		// Fraud detector of clicks that have no corresponding display
		ClickWithoutDisplayDetector detectorClickWithoutDisplay = new ClickWithoutDisplayDetector();
		// Fraud detector of clicks that are too close in time to their corresponding display
		TooQuickClickDetector detectorTooQuickClick = new TooQuickClickDetector();
		// Fraud detector of clicks belonging to ips having a click rate that is too high
		HyperactiveIpDetector detectorHyperactiveIp = new HyperactiveIpDetector();

		// Intial stream sum
		SingleOutputStreamOperator<Tuple2<String, Integer>> streamSum = events.map(new MapFunction<Event, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Event event) throws Exception {
				return new Tuple2<String, Integer>(event.getEventType(), 1);
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple2<String, Integer>(x.f0,  x.f1 + y.f1));

		// Pattern 1 - Clicks without display
		DataStream<Event> streamClickWithoutDisplay = datastream
				.keyBy(Event::getUid)
				.process(detectorClickWithoutDisplay)
				.name("fraud-detector-clickWithoutDisplay")
				.setParallelism(1);

		// Pattern 2 - Clicks too quick
		DataStream<Event> streamTooQuickClick = datastream
				.keyBy(Event::getImpressionId)
				.process(detectorTooQuickClick)
				.name("fraud-detector-tooQuickClick")
				.setParallelism(1);

		// Pattern 3 - Hyperactive IPs
		DataStream<Event> streamHyperactiveIp = datastream
				.keyBy(Event::getIp)
				.process(detectorHyperactiveIp)
				.name("fraud-detector-hyperactiveIp")
				.setParallelism(1);

		// Create sinks to files
		final StreamingFileSink<Event> sinkPattern1 = StreamingFileSink
				.forRowFormat(outputPathPattern1, new SimpleStringEncoder<Event>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();
		final StreamingFileSink<Event> sinkPattern2 = StreamingFileSink
				.forRowFormat(outputPathPattern2, new SimpleStringEncoder<Event>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();
		final StreamingFileSink<Event> sinkPattern3 = StreamingFileSink
				.forRowFormat(outputPathPattern3, new SimpleStringEncoder<Event>("UTF-8"))
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

		// Sum of clicks collected by the different patterns (contains overlap)
		SingleOutputStreamOperator<Tuple3<String, Integer, String>> sumPattern1 = streamClickWithoutDisplay.map(new MapFunction<Event, Tuple3<String, Integer, String>>() {
			@Override
			public Tuple3<String, Integer, String> map(Event event) throws Exception {
				return new Tuple3<String, Integer, String>(event.getEventType(), 1, "Pattern 1");
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple3<String, Integer, String>(x.f0,  x.f1 + y.f1, x.f2));
		SingleOutputStreamOperator<Tuple3<String, Integer, String>> sumPattern2 = streamTooQuickClick.map(new MapFunction<Event, Tuple3<String, Integer, String>>() {
			@Override
			public Tuple3<String, Integer, String> map(Event event) throws Exception {
				return new Tuple3<String, Integer, String>(event.getEventType(), 1, "Pattern 2");
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple3<String, Integer, String>(x.f0,  x.f1 + y.f1, x.f2));
		SingleOutputStreamOperator<Tuple3<String, Integer, String>> sumPattern3 = streamHyperactiveIp.map(new MapFunction<Event, Tuple3<String, Integer, String>>() {
			@Override
			public Tuple3<String, Integer, String> map(Event event) throws Exception {
				return new Tuple3<String, Integer, String>(event.getEventType(), 1, "Pattern 3");
			}
		})
				.keyBy(x -> x.f0)
				.reduce((x, y) -> new Tuple3<String, Integer, String>(x.f0,  x.f1 + y.f1, x.f2));

		// Add sinks
		events.addSink(sink);
		streamClickWithoutDisplay.addSink(sinkPattern1);
		streamTooQuickClick.addSink(sinkPattern2);
		streamHyperactiveIp.addSink(sinkPattern3);

		// Print patterns' output
		//streamClickWithoutDisplay.print();
		//streamTooQuickClick.print();
		//streamHyperactiveIp.print();

		// Print the sum of clicks and displays after every event (for debugging)
		streamSum.print();
		sumPattern1.print();
		sumPattern2.print();
		sumPattern3.print();

		// Execute
		env.execute("Flink Streaming Java API to detect fraudulent clicks in ads.");
	}
}
