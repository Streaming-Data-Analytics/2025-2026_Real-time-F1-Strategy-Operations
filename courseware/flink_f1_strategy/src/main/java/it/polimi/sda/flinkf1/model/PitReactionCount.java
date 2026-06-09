package it.polimi.sda.flinkf1.model;

public class PitReactionCount {
    public String race;
    public long windowStartMs;
    public long windowEndMs;
    public long count;

    public PitReactionCount() {
    }

    public PitReactionCount(String race, long windowStartMs, long windowEndMs, long count) {
        this.race = race;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.count = count;
    }

    @Override
    public String toString() {
        return "PitReactionCount{race=" + race
                + ", windowStartMs=" + windowStartMs
                + ", windowEndMs=" + windowEndMs
                + ", count=" + count
                + "}";
    }
}
