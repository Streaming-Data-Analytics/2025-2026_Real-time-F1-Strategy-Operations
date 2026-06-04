package it.polimi.sda.flinkf1.exercises;

import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.RiskyLap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public final class Q2SlowRiskyLapFilter {
    private Q2SlowRiskyLapFilter() {
    }

    public static DataStream<RiskyLap> build(StreamExecutionEnvironment executionEnvironment,
                                             List<LapEvent> lapEvents) {
        return executionEnvironment
                .fromCollection(lapEvents)
                .filter(lapEvent -> lapEvent.tyreLife >= 5 && lapEvent.lapTimeMs > 83500)
                .map(lapEvent -> new RiskyLap(
                        lapEvent.race,
                        lapEvent.driver,
                        lapEvent.lapNumber,
                        lapEvent.lapTimeMs,
                        lapEvent.tyreLife,
                        lapEvent.compound));
    }
}
