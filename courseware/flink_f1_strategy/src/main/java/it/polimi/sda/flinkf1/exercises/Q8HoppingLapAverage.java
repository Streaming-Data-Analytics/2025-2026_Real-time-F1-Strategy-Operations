package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public final class Q8HoppingLapAverage {
    private Q8HoppingLapAverage() {
    }

    public static DataStream<LapTimeAverage> build(StreamExecutionEnvironment executionEnvironment,
                                                   List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .assignTimestampsAndWatermarks(LapAverageFunctions.lapEventWatermarks())
                .keyBy(LapAverageFunctions::driverKey)
                .window(SlidingEventTimeWindows.of(Time.minutes(4), Time.minutes(2)))
                .process(new LapAverageFunctions.EventTimeAverageWindowFunction());
    }
}
