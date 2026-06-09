package it.polimi.sda.flinkf1.exercises;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;

public final class Q6SlidingLapAverage {

    private static final Duration IN_ORDER_STREAM = Duration.ZERO;

    private Q6SlidingLapAverage() {
    }

    public static DataStream<LapTimeAverage> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                // event-time windows use lapEvent.eventTimeMs, not arrival order
                .assignTimestampsAndWatermarks(lapEventWatermarks())
                // each race/driver pair gets independent windows
                .keyBy(Q6SlidingLapAverage::driverKey)
                // one lap can belong to several overlapping windows
                .window(SlidingEventTimeWindows.of(Time.minutes(4), Time.minutes(1)))
                .process(new EventTimeAverageWindowFunction());
    }

    static WatermarkStrategy<LapEvent> lapEventWatermarks() {
        return WatermarkStrategy
                .<LapEvent>forBoundedOutOfOrderness(IN_ORDER_STREAM)
                .withTimestampAssigner((lapEvent, previousTimestamp) -> lapEvent.eventTimeMs);
    }

    static String driverKey(LapEvent lapEvent) {
        return lapEvent.race + "|" + lapEvent.driver;
    }

    static final class EventTimeAverageWindowFunction
            extends ProcessWindowFunction<LapEvent, LapTimeAverage, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<LapEvent> input,
                Collector<LapTimeAverage> collector) {
            AverageAccumulator accumulator = new AverageAccumulator();
            // input contains the events in the completed window
            for (LapEvent lapEvent : input) {
                accumulator.add(lapEvent);
            }

            collector.collect(new LapTimeAverage(
                    accumulator.race,
                    accumulator.driver,
                    context.window().getStart(),
                    context.window().getEnd(),
                    accumulator.count,
                    accumulator.average()));
        }
    }

    static final class AverageAccumulator {

        private String race;
        private String driver;
        private long count;
        private long lapTimeSumMs;

        private void add(LapEvent lapEvent) {
            race = lapEvent.race;
            driver = lapEvent.driver;
            count++;
            lapTimeSumMs += lapEvent.lapTimeMs;
        }

        private double average() {
            return count == 0 ? 0.0 : (double) lapTimeSumMs / count;
        }
    }
}
