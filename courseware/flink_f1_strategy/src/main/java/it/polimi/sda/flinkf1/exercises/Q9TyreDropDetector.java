package it.polimi.sda.flinkf1.exercises;

import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.TyreDropAlert;

public final class Q9TyreDropDetector {

    private static final double SLOW_LAP_FACTOR = 1.02;
    private static final int ALERT_SLOW_LAPS = 2;
    private static final String BEST_LAP_TIME_STATE = "bestLapTimeMs";
    private static final String CONSECUTIVE_SLOW_LAPS_STATE = "consecutiveSlowLaps";

    private Q9TyreDropDetector() {
    }

    public static DataStream<TyreDropAlert> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .keyBy(Q9TyreDropDetector::driverKey)
                .process(new TyreDropProcessFunction());
    }

    static String driverKey(LapEvent lapEvent) {
        return lapEvent.race + "|" + lapEvent.driver;
    }

    static final class TyreDropProcessFunction
            extends KeyedProcessFunction<String, LapEvent, TyreDropAlert> {

        // one benchmark and one streak are stored per keyed driver
        private transient ValueState<Long> bestLapTimeMsState;
        private transient ValueState<Integer> consecutiveSlowLapsState;

        @Override
        public void open(Configuration configuration) {
            bestLapTimeMsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(BEST_LAP_TIME_STATE, Long.class));
            consecutiveSlowLapsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(CONSECUTIVE_SLOW_LAPS_STATE, Integer.class));
        }

        @Override
        public void processElement(LapEvent lapEvent, Context context,
                Collector<TyreDropAlert> collector) throws Exception {
            Long bestLapTimeMs = bestLapTimeMsState.value();
            Integer previousSlowLaps = consecutiveSlowLapsState.value();

            if (bestLapTimeMs == null) {
                // the first lap initializes the benchmark; it cannot be slow yet
                bestLapTimeMsState.update(lapEvent.lapTimeMs);
                consecutiveSlowLapsState.update(0);
                return;
            }

            // compare with the best lap before possibly updating the benchmark
            boolean slowLap = isSlowComparedWithBest(lapEvent, bestLapTimeMs);
            int consecutiveSlowLaps = slowLap ? valueOrZero(previousSlowLaps) + 1 : 0;

            if (isNewBestLap(lapEvent, bestLapTimeMs)) {
                // a new record becomes the next benchmark
                bestLapTimeMs = lapEvent.lapTimeMs;
                bestLapTimeMsState.update(bestLapTimeMs);
            }

            consecutiveSlowLapsState.update(consecutiveSlowLaps);

            if (shouldEmitAlert(consecutiveSlowLaps)) {
                // emit only when the streak first reaches the threshold
                collector.collect(new TyreDropAlert(
                        lapEvent.race,
                        lapEvent.driver,
                        lapEvent.lapNumber,
                        lapEvent.lapTimeMs,
                        bestLapTimeMs,
                        consecutiveSlowLaps));
            }
        }

        private boolean isSlowComparedWithBest(LapEvent lapEvent, long bestLapTimeMs) {
            return lapEvent.lapTimeMs > bestLapTimeMs * SLOW_LAP_FACTOR;
        }

        private boolean isNewBestLap(LapEvent lapEvent, long bestLapTimeMs) {
            return lapEvent.lapTimeMs < bestLapTimeMs;
        }

        private boolean shouldEmitAlert(int consecutiveSlowLaps) {
            return consecutiveSlowLaps == ALERT_SLOW_LAPS;
        }

        private int valueOrZero(Integer value) {
            return value == null ? 0 : value;
        }
    }
}
