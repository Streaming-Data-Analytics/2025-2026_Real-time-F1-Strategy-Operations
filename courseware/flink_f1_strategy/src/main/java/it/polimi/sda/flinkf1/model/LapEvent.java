package it.polimi.sda.flinkf1.model;

public class LapEvent {
    public long eventTimeMs;
    public String race;
    public String driver;
    public int lapNumber;
    public int position;
    public long lapTimeMs;
    public String compound;
    public int tyreLife;
    public long gapToCarAheadMs;
    public String trackStatus;

    public LapEvent() {
    }

    public LapEvent(long eventTimeMs, String race, String driver, int lapNumber, int position,
                    long lapTimeMs, String compound, int tyreLife, long gapToCarAheadMs,
                    String trackStatus) {
        this.eventTimeMs = eventTimeMs;
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.position = position;
        this.lapTimeMs = lapTimeMs;
        this.compound = compound;
        this.tyreLife = tyreLife;
        this.gapToCarAheadMs = gapToCarAheadMs;
        this.trackStatus = trackStatus;
    }

    @Override
    public String toString() {
        return "LapEvent{race=" + race
                + ", driver=" + driver
                + ", lapNumber=" + lapNumber
                + ", position=" + position
                + ", lapTimeMs=" + lapTimeMs
                + ", compound=" + compound
                + ", tyreLife=" + tyreLife
                + ", gapToCarAheadMs=" + gapToCarAheadMs
                + ", trackStatus=" + trackStatus
                + "}";
    }
}
