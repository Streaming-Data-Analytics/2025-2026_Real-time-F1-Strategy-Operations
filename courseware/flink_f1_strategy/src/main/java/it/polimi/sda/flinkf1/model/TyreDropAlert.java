package it.polimi.sda.flinkf1.model;

public class TyreDropAlert {
    public String race;
    public String driver;
    public int lapNumber;
    public long lapTimeMs;
    public long bestLapTimeMs;
    public int consecutiveSlowLaps;

    public TyreDropAlert() {
    }

    public TyreDropAlert(String race, String driver, int lapNumber, long lapTimeMs,
                         long bestLapTimeMs, int consecutiveSlowLaps) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.lapTimeMs = lapTimeMs;
        this.bestLapTimeMs = bestLapTimeMs;
        this.consecutiveSlowLaps = consecutiveSlowLaps;
    }

    @Override
    public String toString() {
        return "TyreDropAlert{race=" + race
                + ", driver=" + driver
                + ", lapNumber=" + lapNumber
                + ", lapTimeMs=" + lapTimeMs
                + ", bestLapTimeMs=" + bestLapTimeMs
                + ", consecutiveSlowLaps=" + consecutiveSlowLaps
                + "}";
    }
}
