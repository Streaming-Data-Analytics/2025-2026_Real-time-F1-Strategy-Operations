package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public final class Q4TumblingLapAverage {
    private Q4TumblingLapAverage() {
    }

    public static DataStream<LapTimeAverage> build(StreamExecutionEnvironment executionEnvironment,
                                                   List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .assignTimestampsAndWatermarks(LapAverageFunctions.lapEventWatermarks())
                .keyBy(LapAverageFunctions::driverKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .process(new LapAverageFunctions.EventTimeAverageWindowFunction());
    }
}
