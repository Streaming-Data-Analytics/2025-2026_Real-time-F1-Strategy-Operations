package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapCountAverage;
import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

final class LapAverageFunctions {
    private LapAverageFunctions() {
    }

    static WatermarkStrategy<LapEvent> lapEventWatermarks() {
        return WatermarkStrategy
                .<LapEvent>forBoundedOutOfOrderness(Duration.ZERO)
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
                return;
            }

            collector.collect(new LapCountAverage(
                    accumulator.race,
                    accumulator.driver,
                    firstLap,
                    lastLap,
                    accumulator.count,
                    accumulator.average()));
        }
    }

    private static final class AverageAccumulator {
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
