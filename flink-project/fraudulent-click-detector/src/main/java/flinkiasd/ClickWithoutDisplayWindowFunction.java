package flinkiasd;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class ClickWithoutDisplayWindowFunction implements WindowFunction<Event, Integer, String, TimeWindow> {

    //List of fraudulent UIDs to remove
    private List<String> displayedImpressionIds = new ArrayList<>();

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Event> iterable, Collector<Integer> collector) throws Exception {
        System.out.println(s);
        collector.collect(1);

        for (Event event: iterable) {

            if (event.getEventType().equals("display")) {
                if (! displayedImpressionIds.contains(s)) {
                    displayedImpressionIds.add(s);
                }
            } else {
                if (! displayedImpressionIds.contains(s)) {
                    collector.collect(1);
                }
                else {
                    displayedImpressionIds.remove(s);
                }
            }
        }
    }

}
