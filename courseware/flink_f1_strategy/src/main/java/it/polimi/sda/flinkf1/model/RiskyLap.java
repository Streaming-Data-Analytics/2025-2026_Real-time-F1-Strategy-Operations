package it.polimi.sda.flinkf1.model;

public class RiskyLap {
    public String race;
    public String driver;
    public int lapNumber;
    public long lapTimeMs;
    public int tyreLife;
    public String compound;

    public RiskyLap() {
    }

    public RiskyLap(String race, String driver, int lapNumber, long lapTimeMs,
                    int tyreLife, String compound) {
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.lapTimeMs = lapTimeMs;
        this.tyreLife = tyreLife;
        this.compound = compound;
    }

    @Override
    public String toString() {
        return "RiskyLap{race=" + race
                + ", driver=" + driver
                + ", lapNumber=" + lapNumber
                + ", lapTimeMs=" + lapTimeMs
                + ", tyreLife=" + tyreLife
                + ", compound=" + compound
                + "}";
    }
}
