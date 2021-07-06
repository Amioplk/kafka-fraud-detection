package flinkiasd;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClickWithoutDisplayDetector extends KeyedProcessFunction<String, Event, Event> {

    /**
     * Count the number of pending displays per uid every quarter-hour : save a HashMap of number of pending displays
     */
    private transient MapState<String,Integer> pendingDisplayState;

    //List of fraudulent UIDs to remove
    private List<String> impressionIdsToRemove = new ArrayList<>();

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

        if(event.getEventType().equals("click") || event.getEventType().equals("display")) {

            String eventImpressionId = event.getImpressionId();

            if (event.getEventType().equals("display")) {
                if (pendingDisplayState.contains(eventImpressionId)) {
                    pendingDisplayState.put(eventImpressionId, pendingDisplayState.get(eventImpressionId) + 1);
                }
                else {
                    pendingDisplayState.put(eventImpressionId, 1);
                }
            } else {
                if (pendingDisplayState.contains(eventImpressionId)) {
                    if (pendingDisplayState.get(eventImpressionId) <= 0){
                        impressionIdsToRemove.add(eventImpressionId);
                    } else {
                        pendingDisplayState.put(eventImpressionId, pendingDisplayState.get(eventImpressionId) - 1);
                    }
                }
                else {
                    impressionIdsToRemove.add(eventImpressionId);
                }
            }
        }
    }

}
