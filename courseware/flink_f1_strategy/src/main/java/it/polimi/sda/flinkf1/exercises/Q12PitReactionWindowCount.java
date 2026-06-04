package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.PitReactionCount;
import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.StrategyEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public final class Q12PitReactionWindowCount {
    private Q12PitReactionWindowCount() {
    }

    public static DataStream<PitReactionCount> build(StreamExecutionEnvironment executionEnvironment,
                                                     List<StrategyEvent> strategyEvents) {
        WatermarkStrategy<PitReactionMatch> watermarkStrategy = WatermarkStrategy
                .<PitReactionMatch>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((match, previousTimestamp) -> match.pitEventTimeMs);

        return Q10TimedPitPattern.build(executionEnvironment, strategyEvents)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(match -> match.race)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new PitReactionCountProcessWindowFunction());
    }

    private static final class PitReactionCountProcessWindowFunction
            extends ProcessWindowFunction<PitReactionMatch, PitReactionCount, String, TimeWindow> {
        @Override
        public void process(String race, Context context, Iterable<PitReactionMatch> input,
                            Collector<PitReactionCount> collector) {
            long count = 0;
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
