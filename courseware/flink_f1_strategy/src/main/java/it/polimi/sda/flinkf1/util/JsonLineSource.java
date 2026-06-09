package it.polimi.sda.flinkf1.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class JsonLineSource {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonLineSource() {
    }

    public static <T> List<T> read(String relativePath, Class<T> eventType) throws IOException {
        Path jsonlPath = findPath(relativePath);
        List<T> events = new ArrayList<>();

        for (String rawLine : Files.readAllLines(jsonlPath, StandardCharsets.UTF_8)) {
            String line = rawLine.trim();
            if (!line.isEmpty()) {
                events.add(OBJECT_MAPPER.readValue(line, eventType));
            }
        }

        return events;
    }

    private static Path findPath(String relativePath) throws IOException {
        List<Path> candidates = List.of(
                Path.of(relativePath),
                Path.of("courseware", "flink_f1_strategy").resolve(relativePath)
        );

        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                return candidate;
            }
        }

        throw new IOException("Cannot find JSONL file: " + relativePath);
    }
}
