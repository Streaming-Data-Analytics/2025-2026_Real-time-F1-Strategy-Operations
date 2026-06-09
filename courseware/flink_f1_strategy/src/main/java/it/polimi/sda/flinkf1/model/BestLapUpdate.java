package it.polimi.sda.flinkf1.model;

public class BestLapUpdate {
    public String race;
    public String driver;
    public int lapNumber;
    public long bestLapTimeMs;

    public BestLapUpdate() {
    }

    public BestLapUpdate(String race, String driver, int lapNumber, long bestLapTimeMs) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.bestLapTimeMs = bestLapTimeMs;
    }

    @Override
    public String toString() {
        return "BestLapUpdate{race=" + race
                + ", driver=" + driver
                + ", lapNumber=" + lapNumber
                + ", bestLapTimeMs=" + bestLapTimeMs
                + "}";
    }
}
