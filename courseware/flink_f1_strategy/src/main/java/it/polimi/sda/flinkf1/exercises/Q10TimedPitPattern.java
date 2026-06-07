package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.StrategyEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public final class Q10TimedPitPattern {
    static final String TYRE_DROP = "TYRE_DROP";
    static final String PIT = "PIT";
    static final String WINDOW_CLOSED = "WINDOW_CLOSED";
    static final String DROP_STEP = "drop";
    static final String PIT_STEP = "pit";
    static final String CLOSED_STEP = "closed";
    private static final Duration IN_ORDER_STREAM = Duration.ZERO;

    private Q10TimedPitPattern() {
    }

    public static DataStream<PitReactionMatch> build(StreamExecutionEnvironment executionEnvironment,
                                                     List<StrategyEvent> strategyEvents) {
        // TODO
    }

    static Pattern<StrategyEvent, ?> pitReactionPattern() {
        // TODO
    }

    static KeyedStream<StrategyEvent, String> keyedEvents(StreamExecutionEnvironment executionEnvironment,
                                                          List<StrategyEvent> strategyEvents) {
        // TODO
    }

    static WatermarkStrategy<StrategyEvent> strategyEventWatermarks() {
        // TODO
    }

    static DataStream<PitReactionMatch> selectMatches(KeyedStream<StrategyEvent, String> keyedEvents,
                                                      Pattern<StrategyEvent, ?> pattern) {
        PatternStream<StrategyEvent> patternStream = CEP.pattern(keyedEvents, pattern);
        // TODO
    }

    static String strategyKey(StrategyEvent strategyEvent) {
        // TODO
    }

    static final class EventTypeCondition extends SimpleCondition<StrategyEvent> {
        private final String expectedType;

        EventTypeCondition(String expectedType) {
            this.expectedType = expectedType;
        }

        @Override
        public boolean filter(StrategyEvent strategyEvent) {
            // TODO
        }
    }

    static final class PitReactionSelectFunction
            implements PatternSelectFunction<StrategyEvent, PitReactionMatch> {
        @Override
        public PitReactionMatch select(Map<String, List<StrategyEvent>> pattern) {
            // TODO
        }
    }
}
