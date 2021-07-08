package flinkiasd;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;

public class HyperactiveIpDetector extends KeyedProcessFunction<String, Event, Event> {

    /**
     * Store the count of clicks per IP adress
     */
    private transient MapState<String,Integer> clickCountState;
    /**
     * Store the count of displays per IP adress
     */
    private transient MapState<String,Integer> displayCountState;

    /**
     * Once the IP is considered suspicious, collect it
     */
    private List<String> ipsToRemove = new ArrayList<>();

    private final int windowSize = 30*60;
    private final int clickThreshold = 10;
    private final double ctrThreshold = 0.3;

    private long beginTimestamp = (long) -1.0;
    private long endingTimestamp = (long) -1.0;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String,Integer> clickCountStateDescriptor = new MapStateDescriptor<String,Integer>(
                "clickCount",
                Types.STRING,
                Types.INT);
        clickCountState = getRuntimeContext().getMapState(clickCountStateDescriptor);

        MapStateDescriptor<String,Integer> displayCountStateDescriptor = new MapStateDescriptor<String,Integer>(
                "clickCount",
                Types.STRING,
                Types.INT);
        displayCountState = getRuntimeContext().getMapState(displayCountStateDescriptor);
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
            clickCountState.clear();
            displayCountState.clear();
        }

        // Save redundant computation
        String eventIp = event.getIp();

        // If ip is already fraudulent
        if (ipsToRemove.contains(eventIp)){
            collector.collect(event);
        }
        else {
            // If display : just update the display count
            if (event.getEventType().equals("display")) {
                if(displayCountState.contains(eventIp)) {
                    displayCountState.put(eventIp, displayCountState.get(eventIp) + 1);
                }
                else {
                    displayCountState.put(eventIp,1);
                }
            }
            // If click : update the click count and explore pattern
            else {
                // Add the click to the click count
                if(clickCountState.contains(eventIp)) {
                    clickCountState.put(eventIp, clickCountState.get(eventIp) + 1);
                }
                else {
                    clickCountState.put(eventIp, 1);
                }

                // Verify that there is enough clicks to compute CTR with some accuracy
                if (clickCountState.get(eventIp) >= clickThreshold) {
                    if (displayCountState.contains(eventIp)) {
                        // If the CTR is too big, collect the event
                        if (clickCountState.get(eventIp) / displayCountState.get(eventIp) >= ctrThreshold) {
                            ipsToRemove.add(eventIp);
                            collector.collect(event);
                        }
                    }
                    // The else case is probably detected by ClickWithoutDisplayDetector : don't do anything here
                }
            }
        }

    }
}
