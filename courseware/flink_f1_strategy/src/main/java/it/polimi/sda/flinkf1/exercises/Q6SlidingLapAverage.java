package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public final class Q6SlidingLapAverage {
    private static final Duration IN_ORDER_STREAM = Duration.ZERO;

    private Q6SlidingLapAverage() {
    }

    public static DataStream<LapTimeAverage> build(StreamExecutionEnvironment executionEnvironment,
                                                   List<LapEvent> lapEvents) {
        // TODO
    }

    static WatermarkStrategy<LapEvent> lapEventWatermarks() {
        // TODO
    }

    static String driverKey(LapEvent lapEvent) {
        // TODO
    }

    static final class EventTimeAverageWindowFunction
            extends ProcessWindowFunction<LapEvent, LapTimeAverage, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<LapEvent> input,
                            Collector<LapTimeAverage> collector) {
            // TODO
        }
    }

    static final class AverageAccumulator {
        private String race;
        private String driver;
        private long count;
        private long lapTimeSumMs;

        private void add(LapEvent lapEvent) {
            // TODO
        }

        private double average() {
            // TODO
        }
    }
}
