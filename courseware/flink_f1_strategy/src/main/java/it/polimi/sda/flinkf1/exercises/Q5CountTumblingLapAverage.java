package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapCountAverage;
import it.polimi.sda.flinkf1.model.LapEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public final class Q5CountTumblingLapAverage {
    private Q5CountTumblingLapAverage() {
    }

    public static DataStream<LapCountAverage> build(StreamExecutionEnvironment executionEnvironment,
                                                    List<LapEvent> lapEvents) {
        // TODO
    }

    static String driverKey(LapEvent lapEvent) {
        // TODO
    }

    static final class CountAverageWindowFunction
            extends ProcessWindowFunction<LapEvent, LapCountAverage, String, GlobalWindow> {
        private final long minimumCount;

        CountAverageWindowFunction(long minimumCount) {
            this.minimumCount = minimumCount;
        }

        @Override
        public void process(String key, Context context, Iterable<LapEvent> input,
                            Collector<LapCountAverage> collector) {
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
