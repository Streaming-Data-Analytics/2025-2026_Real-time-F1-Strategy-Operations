package it.polimi.sda.flinkf1.model;

import java.util.Locale;

public class LapCountAverage {
    public String race;
    public String driver;
    public int firstLap;
    public int lastLap;
    public long eventCount;
    public double averageLapTimeMs;

    public LapCountAverage() {
    }

    public LapCountAverage(String race, String driver, int firstLap, int lastLap,
                           long eventCount, double averageLapTimeMs) {
        this.race = race;
        this.driver = driver;
        this.firstLap = firstLap;
        this.lastLap = lastLap;
        this.eventCount = eventCount;
        this.averageLapTimeMs = averageLapTimeMs;
    }

    @Override
    public String toString() {
        return "LapCountAverage{race=" + race
                + ", driver=" + driver
                + ", firstLap=" + firstLap
                + ", lastLap=" + lastLap
                + ", eventCount=" + eventCount
                + ", averageLapTimeMs=" + String.format(Locale.ROOT, "%.2f", averageLapTimeMs)
                + "}";
    }
}
