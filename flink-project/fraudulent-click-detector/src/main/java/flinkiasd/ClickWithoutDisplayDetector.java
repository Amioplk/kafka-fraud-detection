package flinkiasd;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class ClickWithoutDisplayDetector extends KeyedProcessFunction<String, Event, Event> {

    /**
     * Count the number of pending displays per uid every quarter-hour : save a HashMap of number of pending displays
     */
    private transient MapState<String,Integer> pendingDisplayState;

    /**
     * Once the Uid is considered suspicious, collect it
     */
    private List<String> uIdsToRemove = new ArrayList<>();

    private final int windowSize = 30*60;

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

        if(beginTimestamp <= 0.0) {
            beginTimestamp = event.getTimestamp();
        }

        if(endingTimestamp <= event.getTimestamp()) {
            endingTimestamp = event.getTimestamp();
        }

        if(endingTimestamp - beginTimestamp >= windowSize) {
            //reset timestamps
            beginTimestamp = event.getTimestamp();
            endingTimestamp = event.getTimestamp();

            // reset map
            pendingDisplayState.clear();
        }

        // Save redundant computation
        String eventUid = event.getUid();

        if (event.getEventType().equals("display")) {
            if (pendingDisplayState.contains(eventUid)) {
                pendingDisplayState.put(eventUid, pendingDisplayState.get(eventUid) + 1);
            } else {
                pendingDisplayState.put(eventUid, 1);
            }
        } else {
            if(uIdsToRemove.contains(eventUid)){
                collector.collect(event);
            }
            else{
                if (pendingDisplayState.contains(eventUid)) {
                    if (pendingDisplayState.get(eventUid) <= 0) {
                        uIdsToRemove.add(eventUid);
                        collector.collect(event);
                    } else {
                        //collector.collect(new Event("{\"eventType\":\"11111111  \", \"uid\":\"4317e35d-4682-4a51-940e-3bcf9cb20ec0\", \"timestamp\":1625692733, \"ip\":\"57.212.4.158\", \"impressionId\": \"8afc5402-1e13-488f-8a0c-db35c9d614a3\"}"));
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
