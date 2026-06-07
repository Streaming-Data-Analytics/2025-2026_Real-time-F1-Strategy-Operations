package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.TyreDropAlert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public final class Q9TyreDropDetector {
    private static final double SLOW_LAP_FACTOR = 1.02;
    private static final int ALERT_SLOW_LAPS = 2;
    private static final String BEST_LAP_TIME_STATE = "bestLapTimeMs";
    private static final String CONSECUTIVE_SLOW_LAPS_STATE = "consecutiveSlowLaps";

    private Q9TyreDropDetector() {
    }

    public static DataStream<TyreDropAlert> build(StreamExecutionEnvironment executionEnvironment,
                                                  List<LapEvent> lapEvents) {
        // TODO
    }

    static String driverKey(LapEvent lapEvent) {
        // TODO
    }

    static final class TyreDropProcessFunction
            extends KeyedProcessFunction<String, LapEvent, TyreDropAlert> {
        private transient ValueState<Long> bestLapTimeMsState;
        private transient ValueState<Integer> consecutiveSlowLapsState;

        @Override
        public void open(Configuration configuration) {
            // Best lap and slow-lap streak are independent for each race + driver key.
            // TODO
        }

        @Override
        public void processElement(LapEvent lapEvent, Context context,
                                   Collector<TyreDropAlert> collector) throws Exception {
            Long bestLapTimeMs = bestLapTimeMsState.value();
            Integer previousSlowLaps = consecutiveSlowLapsState.value();
            // TODO
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
