package it.polimi.sda.flinkf1.exercises;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.PitReactionCount;
import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.StrategyEvent;

public final class Q12PitReactionWindowCount {

    private static final Duration IN_ORDER_MATCH_STREAM = Duration.ZERO;

    private Q12PitReactionWindowCount() {
    }

    public static DataStream<PitReactionCount> build(StreamExecutionEnvironment executionEnvironment,
            List<StrategyEvent> strategyEvents) {
        return Q10TimedPitPattern.build(executionEnvironment, strategyEvents)
                // the match time is the PIT event time, when the reaction is known
                .assignTimestampsAndWatermarks(pitReactionWatermarks())
                // count reactions per race
                .keyBy(match -> match.race)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new PitReactionCountProcessWindowFunction());
    }

    static WatermarkStrategy<PitReactionMatch> pitReactionWatermarks() {
        return WatermarkStrategy
                .<PitReactionMatch>forBoundedOutOfOrderness(IN_ORDER_MATCH_STREAM)
                .withTimestampAssigner((match, previousTimestamp) -> match.pitEventTimeMs);
    }

    static final class PitReactionCountProcessWindowFunction
            extends ProcessWindowFunction<PitReactionMatch, PitReactionCount, String, TimeWindow> {

        @Override
        public void process(String race, Context context, Iterable<PitReactionMatch> input,
                Collector<PitReactionCount> collector) {
            long count = 0;
            // each element is one pit-reaction match from Q10
            for (PitReactionMatch ignored : input) {
                count++;
            }

            collector.collect(new PitReactionCount(
                    race,
                    context.window().getStart(),
                    context.window().getEnd(),
                    count));
        }
    }
}
