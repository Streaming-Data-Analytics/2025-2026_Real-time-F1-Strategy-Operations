package it.polimi.sda.flinkf1.exercises;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.RiskyLap;

public final class Q2SlowRiskyLapFilter {

    static final int MIN_RISKY_TYRE_LIFE = 5;
    static final long SLOW_LAP_TIME_MS = 83_500;

    private Q2SlowRiskyLapFilter() {
    }

    public static DataStream<RiskyLap> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        return executionEnvironment
                // build a bounded stream from the rows loaded by the job
                .fromCollection(lapEvents)
                .filter(new RiskyLapCondition())
                .map(new RiskyLapEvent());
    }

    static final class RiskyLapCondition implements FilterFunction<LapEvent> {

        @Override
        public boolean filter(LapEvent lapEvent) {
            // both conditions must hold for the lap to stay in the stream
            return lapEvent.tyreLife >= MIN_RISKY_TYRE_LIFE
                    && lapEvent.lapTimeMs > SLOW_LAP_TIME_MS;
        }
    }

    static final class RiskyLapEvent implements MapFunction<LapEvent, RiskyLap> {

        @Override
        public RiskyLap map(LapEvent lapEvent) {
            // keep only the fields printed by the risky-lap result stream
            return new RiskyLap(
                    lapEvent.race,
                    lapEvent.driver,
                    lapEvent.lapNumber,
                    lapEvent.lapTimeMs,
                    lapEvent.tyreLife,
                    lapEvent.compound);
        }
    }
}
