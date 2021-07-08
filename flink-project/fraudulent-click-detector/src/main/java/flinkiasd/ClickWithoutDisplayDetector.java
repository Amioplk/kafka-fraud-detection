package flinkiasd;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * --- PATTERN 1 ---
 *
 * Class that collects the clicks that do not have a display with the same uid beforehand.
 *
 * Note : Class aimed to be used in the method KeyedStream.process()
 */
public class ClickWithoutDisplayDetector extends KeyedProcessFunction<String, Event, Event> {

    /**
     * Count the number of pending displays per uid every quarter-hour : save a HashMap of number of pending displays
     */
    private transient MapState<String,Integer> pendingDisplayState;

    /**
     * Once the Uid is considered suspicious, collect it
     */
    private List<String> uIdsToRemove = new ArrayList<>();

    /**
     * Window size in seconds of our hand-made window
     */
    private final int windowSize = 30*60;

    /**
     * Bounds to use for our hand-made window
     */
    private long beginTimestamp = (long) -1.0;
    private long endingTimestamp = (long) -1.0;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String,Integer> pendingDisplayStateDescriptor = new MapStateDescriptor<String,Integer>(
                "pendingDisplays",
                Types.STRING,
                Types.INT);
        pendingDisplayState = getRuntimeContext().getMapState(pendingDisplayStateDescriptor);
    }

    @Override
    public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {

        // Initialize lower bound
        if(beginTimestamp <= 0.0) {
            beginTimestamp = event.getTimestamp();
        }

        // Update upper bound
        if(endingTimestamp < event.getTimestamp()) {
            endingTimestamp = event.getTimestamp();
        }

        // End of the tumbling window
        if(endingTimestamp - beginTimestamp >= windowSize) {
            //reset timestamps
            beginTimestamp = event.getTimestamp();
            endingTimestamp = event.getTimestamp();

            // reset map
            pendingDisplayState.clear();
        }

        // Save redundant computation
        String eventUid = event.getUid();

        // If display : increment the number of pending displays for the current uid
        if (event.getEventType().equals("display")) {
            if (pendingDisplayState.contains(eventUid)) {
                pendingDisplayState.put(eventUid, pendingDisplayState.get(eventUid) + 1);
            } else {
                pendingDisplayState.put(eventUid, 1);
            }
        }
        // If click :
        else {
            // If uid is already identified as fraudulent : collect the click
            if(uIdsToRemove.contains(eventUid)){
                collector.collect(event);
            }
            else{
                if (pendingDisplayState.contains(eventUid)) {
                    // If there is no pending display : collect the click
                    if (pendingDisplayState.get(eventUid) <= 0) {
                        uIdsToRemove.add(eventUid);
                        collector.collect(event);
                    }
                    // If there is at least 1 pending display : decrement pending displays for this uid
                    else {
                        pendingDisplayState.put(eventUid, pendingDisplayState.get(eventUid) - 1);
                    }
                } else {
                    uIdsToRemove.add(eventUid);
                    collector.collect(event);
                }
            }
        }
    }

}
