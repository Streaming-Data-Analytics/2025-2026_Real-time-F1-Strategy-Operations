package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapCountAverage;
import it.polimi.sda.flinkf1.model.LapEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public final class Q7CountSlidingLapAverage {
    private Q7CountSlidingLapAverage() {
    }

    public static DataStream<LapCountAverage> build(StreamExecutionEnvironment executionEnvironment,
                                                    List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .keyBy(LapAverageFunctions::driverKey)
                .countWindow(3, 1)
                .process(new LapAverageFunctions.CountAverageWindowFunction(3));
    }
}
