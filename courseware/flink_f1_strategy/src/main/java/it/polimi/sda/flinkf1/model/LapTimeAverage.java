package it.polimi.sda.flinkf1.model;

import java.util.Locale;

public class LapTimeAverage {
    public String race;
    public String driver;
    public long windowStartMs;
    public long windowEndMs;
    public long eventCount;
    public double averageLapTimeMs;

    public LapTimeAverage() {
    }

    public LapTimeAverage(String race, String driver, long windowStartMs, long windowEndMs,
                          long eventCount, double averageLapTimeMs) {
        this.race = race;
        this.driver = driver;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.eventCount = eventCount;
        this.averageLapTimeMs = averageLapTimeMs;
    }

    @Override
    public String toString() {
        return "LapTimeAverage{race=" + race
                + ", driver=" + driver
                + ", windowStartMs=" + windowStartMs
                + ", windowEndMs=" + windowEndMs
                + ", eventCount=" + eventCount
                + ", averageLapTimeMs=" + String.format(Locale.ROOT, "%.2f", averageLapTimeMs)
                + "}";
    }
}
