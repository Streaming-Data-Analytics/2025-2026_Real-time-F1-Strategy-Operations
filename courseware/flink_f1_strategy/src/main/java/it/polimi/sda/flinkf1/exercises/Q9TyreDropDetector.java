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

    private Q9TyreDropDetector() {
    }

    public static DataStream<TyreDropAlert> build(StreamExecutionEnvironment executionEnvironment,
                                                  List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .keyBy(LapAverageFunctions::driverKey)
                .process(new TyreDropProcessFunction());
    }

    private static final class TyreDropProcessFunction
            extends KeyedProcessFunction<String, LapEvent, TyreDropAlert> {
        private transient ValueState<Long> bestLapTimeMsState;
        private transient ValueState<Integer> consecutiveSlowLapsState;

        @Override
        public void open(Configuration configuration) {
            bestLapTimeMsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("bestLapTimeMs", Long.class));
            consecutiveSlowLapsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("consecutiveSlowLaps", Integer.class));
        }

        @Override
        public void processElement(LapEvent lapEvent, Context context,
                                   Collector<TyreDropAlert> collector) throws Exception {
            Long bestLapTimeMs = bestLapTimeMsState.value();
            Integer previousSlowLaps = consecutiveSlowLapsState.value();

            if (bestLapTimeMs == null) {
                bestLapTimeMsState.update(lapEvent.lapTimeMs);
                consecutiveSlowLapsState.update(0);
                return;
            }

            boolean slowLap = lapEvent.lapTimeMs > bestLapTimeMs * SLOW_LAP_FACTOR;
            int consecutiveSlowLaps = slowLap ? valueOrZero(previousSlowLaps) + 1 : 0;

            if (lapEvent.lapTimeMs < bestLapTimeMs) {
                bestLapTimeMs = lapEvent.lapTimeMs;
                bestLapTimeMsState.update(bestLapTimeMs);
            }

            consecutiveSlowLapsState.update(consecutiveSlowLaps);

            if (consecutiveSlowLaps == ALERT_SLOW_LAPS) {
                collector.collect(new TyreDropAlert(
                        lapEvent.race,
                        lapEvent.driver,
                        lapEvent.lapNumber,
                        lapEvent.lapTimeMs,
                        bestLapTimeMs,
                        consecutiveSlowLaps));
            }
        }

        private int valueOrZero(Integer value) {
            return value == null ? 0 : value;
        }
    }
}
