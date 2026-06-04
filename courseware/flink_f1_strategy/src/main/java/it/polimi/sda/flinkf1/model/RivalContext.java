package it.polimi.sda.flinkf1.model;

public class RivalContext {
    public String race;
    public int lapNumber;
    public String driver;
    public int position;
    public String driverAhead;
    public String driverBehind;
    public long gapToCarAheadMs;

    public RivalContext() {
    }

    public RivalContext(String race, int lapNumber, String driver, int position,
                        String driverAhead, String driverBehind, long gapToCarAheadMs) {
        this.race = race;
        this.lapNumber = lapNumber;
        this.driver = driver;
        this.position = position;
        this.driverAhead = driverAhead;
        this.driverBehind = driverBehind;
        this.gapToCarAheadMs = gapToCarAheadMs;
    }

    @Override
    public String toString() {
        return "RivalContext{race=" + race
                + ", lapNumber=" + lapNumber
                + ", driver=" + driver
                + ", position=" + position
                + ", ahead=" + driverAhead
                + ", behind=" + driverBehind
                + ", gapToCarAheadMs=" + gapToCarAheadMs
                + "}";
    }
}
