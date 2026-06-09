package it.polimi.sda.flinkf1.exercises;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.RivalContext;

public final class BonusRivalContextWindow {

    private static final String NO_DRIVER = "NONE";

    private BonusRivalContextWindow() {
    }

    public static DataStream<RivalContext> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        // race time groups lap completions that happen close together
        WatermarkStrategy<LapEvent> watermarkStrategy = WatermarkStrategy
                .<LapEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((lapEvent, previousTimestamp) -> lapEvent.eventTimeMs);

        return executionEnvironment
                .fromCollection(lapEvents)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // the key is race and lap, so all drivers on the lap meet in one window
                .keyBy(BonusRivalContextWindow::lapKey)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .process(new RivalContextProcessWindowFunction());
    }

    private static String lapKey(LapEvent lapEvent) {
        return lapEvent.race + "|" + lapEvent.lapNumber;
    }

    private static final class RivalContextProcessWindowFunction
            extends ProcessWindowFunction<LapEvent, RivalContext, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<LapEvent> input,
                Collector<RivalContext> collector) {
            List<LapEvent> lapEvents = new ArrayList<>();
            // sort after collecting because window input is an iterable
            input.forEach(lapEvents::add);
            lapEvents.sort(Comparator
                    .comparingInt((LapEvent lapEvent) -> lapEvent.position)
                    .thenComparing(lapEvent -> lapEvent.driver));

            for (int index = 0; index < lapEvents.size(); index++) {
                LapEvent lapEvent = lapEvents.get(index);
                // neighbors in sorted order give the direct rivals
                String driverAhead = index == 0 ? NO_DRIVER : lapEvents.get(index - 1).driver;
                String driverBehind = index == lapEvents.size() - 1
                        ? NO_DRIVER
                        : lapEvents.get(index + 1).driver;

                collector.collect(new RivalContext(
                        lapEvent.race,
                        lapEvent.lapNumber,
                        lapEvent.driver,
                        lapEvent.position,
                        driverAhead,
                        driverBehind,
                        lapEvent.gapToCarAheadMs));
            }
        }
    }
}
