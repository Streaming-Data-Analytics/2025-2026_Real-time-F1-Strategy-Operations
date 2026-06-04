package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.BestLapUpdate;
import it.polimi.sda.flinkf1.model.LapEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public final class Q3BestLapByDriver {
    private Q3BestLapByDriver() {
    }

    public static DataStream<BestLapUpdate> build(StreamExecutionEnvironment executionEnvironment,
                                                  List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .keyBy(LapAverageFunctions::driverKey)
                .process(new BestLapProcessFunction());
    }

    private static final class BestLapProcessFunction
            extends KeyedProcessFunction<String, LapEvent, BestLapUpdate> {
        private transient ValueState<Long> bestLapTimeMsState;

        @Override
        public void open(Configuration configuration) {
            bestLapTimeMsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("bestLapTimeMs", Long.class));
        }

        @Override
        public void processElement(LapEvent lapEvent, Context context,
                                   Collector<BestLapUpdate> collector) throws Exception {
            Long bestLapTimeMs = bestLapTimeMsState.value();
            if (bestLapTimeMs == null || lapEvent.lapTimeMs < bestLapTimeMs) {
                bestLapTimeMsState.update(lapEvent.lapTimeMs);
                collector.collect(new BestLapUpdate(
                        lapEvent.race,
                        lapEvent.driver,
                        lapEvent.lapNumber,
                        lapEvent.lapTimeMs));
            }
        }
    }
}
