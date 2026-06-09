package it.polimi.sda.flinkf1.exercises;

import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.LapCountAverage;
import it.polimi.sda.flinkf1.model.LapEvent;

public final class Q5CountTumblingLapAverage {

    private Q5CountTumblingLapAverage() {
    }

    public static DataStream<LapCountAverage> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                // count windows are built separately for each race/driver
                .keyBy(Q5CountTumblingLapAverage::driverKey)
                // after three events for the key, Flink closes the window
                .countWindow(3)
                .process(new CountAverageWindowFunction(3));
    }

    static String driverKey(LapEvent lapEvent) {
        return lapEvent.race + "|" + lapEvent.driver;
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
            AverageAccumulator accumulator = new AverageAccumulator();
            int firstLap = Integer.MAX_VALUE;
            int lastLap = Integer.MIN_VALUE;

            for (LapEvent lapEvent : input) {
                accumulator.add(lapEvent);
                firstLap = Math.min(firstLap, lapEvent.lapNumber);
                lastLap = Math.max(lastLap, lapEvent.lapNumber);
            }

            if (accumulator.count < minimumCount) {
                // keep the function reusable for windows that may emit early
                return;
            }

            // lap numbers make the physical window visible in the output
            collector.collect(new LapCountAverage(
                    accumulator.race,
                    accumulator.driver,
                    firstLap,
                    lastLap,
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
