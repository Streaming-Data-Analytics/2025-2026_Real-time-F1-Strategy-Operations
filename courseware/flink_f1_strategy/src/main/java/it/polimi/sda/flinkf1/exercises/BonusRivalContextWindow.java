package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.RivalContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class BonusRivalContextWindow {
    private static final String NO_DRIVER = "NONE";

    private BonusRivalContextWindow() {
    }

    public static DataStream<RivalContext> build(StreamExecutionEnvironment executionEnvironment,
                                                 List<LapEvent> lapEvents) {
        // TODO
    }

    private static String lapKey(LapEvent lapEvent) {
        // TODO
    }

    private static final class RivalContextProcessWindowFunction
            extends ProcessWindowFunction<LapEvent, RivalContext, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<LapEvent> input,
                            Collector<RivalContext> collector) {
            // TODO
        }
    }
}
