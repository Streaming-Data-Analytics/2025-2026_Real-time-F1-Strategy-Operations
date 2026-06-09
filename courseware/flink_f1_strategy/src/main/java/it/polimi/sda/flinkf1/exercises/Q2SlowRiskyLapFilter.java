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
        // TODO
    }

    static final class RiskyLapCondition implements FilterFunction<LapEvent> {

        @Override
        public boolean filter(LapEvent lapEvent) {
            // TODO
        }
    }

    static final class RiskyLapEvent implements MapFunction<LapEvent, RiskyLap> {

        @Override
        public RiskyLap map(LapEvent lapEvent) {
            // TODO
        }
    }
}
