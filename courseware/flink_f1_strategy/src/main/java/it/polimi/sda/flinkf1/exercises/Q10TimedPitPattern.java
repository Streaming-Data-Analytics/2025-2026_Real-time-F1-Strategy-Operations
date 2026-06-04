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

    private Q10TimedPitPattern() {
    }

    public static DataStream<PitReactionMatch> build(StreamExecutionEnvironment executionEnvironment,
                                                     List<StrategyEvent> strategyEvents) {
        Pattern<StrategyEvent, ?> pitReactionPattern = Pattern
                .<StrategyEvent>begin("drop")
                .where(new EventTypeCondition(TYRE_DROP))
                .followedBy("pit")
                .where(new EventTypeCondition(PIT))
                .within(Time.minutes(3));

        return selectMatches(keyedEvents(executionEnvironment, strategyEvents), pitReactionPattern);
    }

    static KeyedStream<StrategyEvent, String> keyedEvents(StreamExecutionEnvironment executionEnvironment,
                                                          List<StrategyEvent> strategyEvents) {
        WatermarkStrategy<StrategyEvent> watermarkStrategy = WatermarkStrategy
                .<StrategyEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((strategyEvent, previousTimestamp) -> strategyEvent.eventTimeMs);

        return executionEnvironment
                .fromCollection(strategyEvents)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(Q10TimedPitPattern::strategyKey);
    }

    static DataStream<PitReactionMatch> selectMatches(KeyedStream<StrategyEvent, String> keyedEvents,
                                                      Pattern<StrategyEvent, ?> pattern) {
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

    private static final class PitReactionSelectFunction
            implements PatternSelectFunction<StrategyEvent, PitReactionMatch> {
        @Override
        public PitReactionMatch select(Map<String, List<StrategyEvent>> pattern) {
            StrategyEvent dropEvent = pattern.get("drop").get(0);
            StrategyEvent pitEvent = pattern.get("pit").get(0);
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
