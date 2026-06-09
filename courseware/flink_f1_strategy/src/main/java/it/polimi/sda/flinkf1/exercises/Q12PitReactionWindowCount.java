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
    private static final Duration IN_ORDER_MATCH_STREAM = Duration.ZERO;

    private Q12PitReactionWindowCount() {
    }

    public static DataStream<PitReactionCount> build(StreamExecutionEnvironment executionEnvironment,
                                                     List<StrategyEvent> strategyEvents) {
        // TODO
    }

    static WatermarkStrategy<PitReactionMatch> pitReactionWatermarks() {
        // TODO
    }

    static final class PitReactionCountProcessWindowFunction
            extends ProcessWindowFunction<PitReactionMatch, PitReactionCount, String, TimeWindow> {
        @Override
        public void process(String race, Context context, Iterable<PitReactionMatch> input,
                            Collector<PitReactionCount> collector) {
            // TODO
        }
    }
}
