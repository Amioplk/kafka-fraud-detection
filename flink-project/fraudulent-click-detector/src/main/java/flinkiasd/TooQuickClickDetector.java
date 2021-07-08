package flinkiasd;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TooQuickClickDetector extends KeyedProcessFunction<String, Event, Event> {

    /**
     * Store the last display of each impressionId
     */
    private transient MapState<String,Long> lastDisplayState;

    private final int windowSize = 30*60;
    private final int humanThreshold = 2;

    private long beginTimestamp = (long) -1.0;
    private long endingTimestamp = (long) -1.0;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String,Long> pendingDisplayStateDescriptor = new MapStateDescriptor<String,Long>(
                "displayTimestamps",
                Types.STRING,
                Types.LONG);
        lastDisplayState = getRuntimeContext().getMapState(pendingDisplayStateDescriptor);
    }

    @Override
    public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {

        if(beginTimestamp <= 0.0) {
            beginTimestamp = event.getTimestamp();
        }

        // Update ending timestamp
        if(endingTimestamp < event.getTimestamp()) {
            endingTimestamp = event.getTimestamp();
        }

        if(endingTimestamp - beginTimestamp >= windowSize) {
            // reset timestamps
            beginTimestamp = event.getTimestamp();
            endingTimestamp = event.getTimestamp();

            // reset
            lastDisplayState.clear();
        }

        // Save redundant computation
        String eventImpressionId = event.getImpressionId();
        Long eventTimestamp = event.getTimestamp();

        // If display : just update last timestamp
        if (event.getEventType().equals("display")) {
            lastDisplayState.put(eventImpressionId, eventTimestamp);
        }
        // If click : verify that the click is not too quick after the display
        else {
            if (lastDisplayState.contains(eventImpressionId)) {
                if (lastDisplayState.get(eventImpressionId) - eventTimestamp < humanThreshold) {
                    collector.collect(event);
                }
            }
            // The else case is probably detected by ClickWithoutDisplayDetector : don't do anything here
        }

    }
}
