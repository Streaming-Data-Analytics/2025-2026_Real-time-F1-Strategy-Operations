package it.polimi.sda.flinkf1.exercises;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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

import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.StrategyEvent;

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
        // prepare the keyed event-time stream, then apply the pattern
        return selectMatches(
                keyedEvents(executionEnvironment, strategyEvents),
                pitReactionPattern());
    }

    static Pattern<StrategyEvent, ?> pitReactionPattern() {
        // require TYRE_DROP followed by PIT within three race-time minutes
        return Pattern
                .<StrategyEvent>begin(DROP_STEP)
                .where(new EventTypeCondition(TYRE_DROP))
                .followedBy(PIT_STEP)
                .where(new EventTypeCondition(PIT))
                .within(Time.minutes(3));
    }

    static KeyedStream<StrategyEvent, String> keyedEvents(StreamExecutionEnvironment executionEnvironment,
            List<StrategyEvent> strategyEvents) {
        return executionEnvironment
                .fromCollection(strategyEvents)
                // use these timestamps to enforce the within clause
                .assignTimestampsAndWatermarks(strategyEventWatermarks())
                .keyBy(Q10TimedPitPattern::strategyKey);
    }

    static WatermarkStrategy<StrategyEvent> strategyEventWatermarks() {
        return WatermarkStrategy
                .<StrategyEvent>forBoundedOutOfOrderness(IN_ORDER_STREAM)
                .withTimestampAssigner((strategyEvent, previousTimestamp) -> strategyEvent.eventTimeMs);
    }

    static DataStream<PitReactionMatch> selectMatches(KeyedStream<StrategyEvent, String> keyedEvents,
            Pattern<StrategyEvent, ?> pattern) {
        // evaluate the pattern separately for each key
        PatternStream<StrategyEvent> patternStream = CEP.pattern(keyedEvents, pattern);
        return patternStream.select(new PitReactionSelectFunction());
    }

    static String strategyKey(StrategyEvent strategyEvent) {
        return strategyEvent.race + "|" + strategyEvent.driver;
    }

    static final class EventTypeCondition extends SimpleCondition<StrategyEvent> {

        private final String expectedType;

        EventTypeCondition(String expectedType) {
            this.expectedType = expectedType;
        }

        @Override
        public boolean filter(StrategyEvent strategyEvent) {
            return strategyEvent.hasType(expectedType);
        }
    }

    static final class PitReactionSelectFunction
            implements PatternSelectFunction<StrategyEvent, PitReactionMatch> {

        @Override
        public PitReactionMatch select(Map<String, List<StrategyEvent>> pattern) {
            // the map contains the events matched by step name
            StrategyEvent dropEvent = pattern.get(DROP_STEP).get(0);
            StrategyEvent pitEvent = pattern.get(PIT_STEP).get(0);
            long delayMs = pitEvent.eventTimeMs - dropEvent.eventTimeMs;

            return new PitReactionMatch(
                    dropEvent.race,
                    dropEvent.driver,
                    dropEvent.lapNumber,
                    pitEvent.lapNumber,
                    pitEvent.lapNumber - dropEvent.lapNumber,
                    delayMs,
                    pitEvent.eventTimeMs);
        }
    }
}
