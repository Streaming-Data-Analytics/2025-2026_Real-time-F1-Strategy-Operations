package it.polimi.sda.flinkf1.model;

public class StrategyEvent {
    public long eventTimeMs;
    public String race;
    public String driver;
    public int lapNumber;
    public String eventType;

    public StrategyEvent() {
    }

    public StrategyEvent(long eventTimeMs, String race, String driver, int lapNumber,
                         String eventType) {
        this.eventTimeMs = eventTimeMs;
        this.race = race;
        this.driver = driver;
        this.lapNumber = lapNumber;
        this.eventType = eventType;
    }

    public boolean hasType(String expectedType) {
        return expectedType.equals(eventType);
    }

    @Override
    public String toString() {
        return "StrategyEvent{race=" + race
                + ", driver=" + driver
                + ", lapNumber=" + lapNumber
                + ", eventType=" + eventType
                + "}";
    }
}
