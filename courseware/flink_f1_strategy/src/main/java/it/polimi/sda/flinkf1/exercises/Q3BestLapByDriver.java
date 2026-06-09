package it.polimi.sda.flinkf1.exercises;

import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import it.polimi.sda.flinkf1.model.BestLapUpdate;
import it.polimi.sda.flinkf1.model.LapEvent;

public final class Q3BestLapByDriver {

    private static final String BEST_LAP_TIME_STATE = "bestLapTimeMs";

    private Q3BestLapByDriver() {
    }

    public static DataStream<BestLapUpdate> build(StreamExecutionEnvironment executionEnvironment,
            List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                // state is scoped by race and driver
                .keyBy(Q3BestLapByDriver::driverKey)
                .process(new BestLapProcessFunction());
    }

    static String driverKey(LapEvent lapEvent) {
        return lapEvent.race + "|" + lapEvent.driver;
    }

    static final class BestLapProcessFunction
            extends KeyedProcessFunction<String, LapEvent, BestLapUpdate> {

        private transient ValueState<Long> bestLapTimeMsState;

        @Override
        public void open(Configuration configuration) {
            // each key gets its own best lap value
            bestLapTimeMsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(BEST_LAP_TIME_STATE, Long.class));
        }

        @Override
        public void processElement(LapEvent lapEvent, Context context,
                Collector<BestLapUpdate> collector) throws Exception {
            Long bestLapTimeMs = bestLapTimeMsState.value();
            // emit only the first lap for a driver and later improvements
            if (isFirstLapForDriver(bestLapTimeMs) || isNewBestLap(lapEvent, bestLapTimeMs)) {
                bestLapTimeMsState.update(lapEvent.lapTimeMs);
                collector.collect(new BestLapUpdate(
                        lapEvent.race,
                        lapEvent.driver,
                        lapEvent.lapNumber,
                        lapEvent.lapTimeMs));
            }
        }

        private boolean isFirstLapForDriver(Long bestLapTimeMs) {
            return bestLapTimeMs == null;
        }

        private boolean isNewBestLap(LapEvent lapEvent, Long bestLapTimeMs) {
            return lapEvent.lapTimeMs < bestLapTimeMs;
        }
    }
}
