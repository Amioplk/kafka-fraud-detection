package flinkiasd;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ClickWithoutDisplayWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

    //List of fraudulent UIDs to remove
    private List<String> displayedImpressionIds = new ArrayList<>();

    @Override
    public void process(String s, Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {

        for (Event event: iterable) {

            System.out.println(s);
            collector.collect(s);

            if (event.getEventType().equals("display")) {
                if (! displayedImpressionIds.contains(s)) {
                    displayedImpressionIds.add(s);
                }
            } else {
                if (! displayedImpressionIds.contains(s)) {
                    collector.collect(s);
                }
                else {
                    displayedImpressionIds.remove(s);
                }
            }
        }
    }
}
