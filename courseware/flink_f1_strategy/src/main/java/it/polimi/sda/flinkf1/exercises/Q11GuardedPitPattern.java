package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.StrategyEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public final class Q11GuardedPitPattern {
    private Q11GuardedPitPattern() {
    }

    public static DataStream<PitReactionMatch> build(StreamExecutionEnvironment executionEnvironment,
                                                     List<StrategyEvent> strategyEvents) {
        Pattern<StrategyEvent, ?> guardedPitReactionPattern = Pattern
                .<StrategyEvent>begin("drop")
                .where(new Q10TimedPitPattern.EventTypeCondition(Q10TimedPitPattern.TYRE_DROP))
                .notFollowedBy("closed")
                .where(new Q10TimedPitPattern.EventTypeCondition(Q10TimedPitPattern.WINDOW_CLOSED))
                .followedBy("pit")
                .where(new Q10TimedPitPattern.EventTypeCondition(Q10TimedPitPattern.PIT))
                .within(Time.minutes(3));

        return Q10TimedPitPattern.selectMatches(
                Q10TimedPitPattern.keyedEvents(executionEnvironment, strategyEvents),
                guardedPitReactionPattern);
    }
}
