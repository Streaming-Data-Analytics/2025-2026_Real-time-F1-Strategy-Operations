package it.polimi.sda.flinkf1.model;

public class PitReactionMatch {
    public String race;
    public String driver;
    public int dropLap;
    public int pitLap;
    public int delayLaps;
    public long delayMs;
    public long pitEventTimeMs;

    public PitReactionMatch() {
    }

    public PitReactionMatch(String race, String driver, int dropLap, int pitLap,
                            int delayLaps, long delayMs, long pitEventTimeMs) {
        this.race = race;
        this.driver = driver;
        this.dropLap = dropLap;
        this.pitLap = pitLap;
        this.delayLaps = delayLaps;
        this.delayMs = delayMs;
        this.pitEventTimeMs = pitEventTimeMs;
    }

    @Override
    public String toString() {
        return "PitReactionMatch{race=" + race
                + ", driver=" + driver
                + ", dropLap=" + dropLap
                + ", pitLap=" + pitLap
                + ", delayLaps=" + delayLaps
                + ", delayMs=" + delayMs
                + "}";
    }
}
