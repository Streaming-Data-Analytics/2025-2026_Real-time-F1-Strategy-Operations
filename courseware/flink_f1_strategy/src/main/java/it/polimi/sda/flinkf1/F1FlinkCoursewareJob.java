package it.polimi.sda.flinkf1;

import it.polimi.sda.flinkf1.exercises.BonusRivalContextWindow;
import it.polimi.sda.flinkf1.exercises.Q10TimedPitPattern;
import it.polimi.sda.flinkf1.exercises.Q11GuardedPitPattern;
import it.polimi.sda.flinkf1.exercises.Q12PitReactionWindowCount;
import it.polimi.sda.flinkf1.exercises.Q2SlowRiskyLapFilter;
import it.polimi.sda.flinkf1.exercises.Q3BestLapByDriver;
import it.polimi.sda.flinkf1.exercises.Q4TumblingLapAverage;
import it.polimi.sda.flinkf1.exercises.Q5CountTumblingLapAverage;
import it.polimi.sda.flinkf1.exercises.Q6SlidingLapAverage;
import it.polimi.sda.flinkf1.exercises.Q7CountSlidingLapAverage;
import it.polimi.sda.flinkf1.exercises.Q8HoppingLapAverage;
import it.polimi.sda.flinkf1.exercises.Q9TyreDropDetector;
import it.polimi.sda.flinkf1.model.BestLapUpdate;
import it.polimi.sda.flinkf1.model.LapCountAverage;
import it.polimi.sda.flinkf1.model.LapEvent;
import it.polimi.sda.flinkf1.model.LapTimeAverage;
import it.polimi.sda.flinkf1.model.PitReactionCount;
import it.polimi.sda.flinkf1.model.PitReactionMatch;
import it.polimi.sda.flinkf1.model.RivalContext;
import it.polimi.sda.flinkf1.model.RiskyLap;
import it.polimi.sda.flinkf1.model.StrategyEvent;
import it.polimi.sda.flinkf1.model.TyreDropAlert;
import it.polimi.sda.flinkf1.util.JsonLineSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class F1FlinkCoursewareJob {
    private static final String FORK_PROPERTY = "flink.f1.courseware.forked";

    private F1FlinkCoursewareJob() {
    }

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setContextClassLoader(F1FlinkCoursewareJob.class.getClassLoader());

        if (!Boolean.getBoolean(FORK_PROPERTY)) {
            runInForkedJvm(args);
            return;
        }

        if (args.length != 1) {
            printUsage();
            return;
        }

        String exercise = args[0].toLowerCase(Locale.ROOT);
        switch (exercise) {
            case "q2" -> runQ2();
            case "q3" -> runQ3();
            case "q4" -> runQ4();
            case "q5" -> runQ5();
            case "q6" -> runQ6();
            case "q7" -> runQ7();
            case "q8" -> runQ8();
            case "q9" -> runQ9();
            case "q10" -> runQ10();
            case "q11" -> runQ11();
            case "q12" -> runQ12();
            case "bonus" -> runBonus();
            case "all" -> {
                runQ2();
                runQ3();
                runQ4();
                runQ5();
                runQ6();
                runQ7();
                runQ8();
                runQ9();
                runQ10();
                runQ11();
                runQ12();
                runBonus();
            }
            default -> printUsage();
        }
    }

    private static void runQ2() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<RiskyLap> riskyLaps = Q2SlowRiskyLapFilter.build(executionEnvironment, lapEvents);
        printResults("Q2", riskyLaps);
    }

    private static void runQ3() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<BestLapUpdate> updates = Q3BestLapByDriver.build(executionEnvironment, lapEvents);
        printResults("Q3", updates);
    }

    private static void runQ4() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<LapTimeAverage> averages = Q4TumblingLapAverage.build(executionEnvironment, lapEvents);
        printResults("Q4", averages);
    }

    private static void runQ5() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<LapCountAverage> averages = Q5CountTumblingLapAverage.build(executionEnvironment, lapEvents);
        printResults("Q5", averages);
    }

    private static void runQ6() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<LapTimeAverage> averages = Q6SlidingLapAverage.build(executionEnvironment, lapEvents);
        printResults("Q6", averages);
    }

    private static void runQ7() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<LapCountAverage> averages = Q7CountSlidingLapAverage.build(executionEnvironment, lapEvents);
        printResults("Q7", averages);
    }

    private static void runQ8() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<LapTimeAverage> averages = Q8HoppingLapAverage.build(executionEnvironment, lapEvents);
        printResults("Q8", averages);
    }

    private static void runQ9() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<TyreDropAlert> alerts = Q9TyreDropDetector.build(executionEnvironment, lapEvents);
        printResults("Q9", alerts);
    }

    private static void runQ10() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<StrategyEvent> strategyEvents = JsonLineSource.read(
                "data/strategy_events.jsonl",
                StrategyEvent.class);
        DataStream<PitReactionMatch> matches = Q10TimedPitPattern.build(
                executionEnvironment,
                strategyEvents);
        printResults("Q10", matches);
    }

    private static void runQ11() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<StrategyEvent> strategyEvents = JsonLineSource.read(
                "data/strategy_events.jsonl",
                StrategyEvent.class);
        DataStream<PitReactionMatch> matches = Q11GuardedPitPattern.build(
                executionEnvironment,
                strategyEvents);
        printResults("Q11", matches);
    }

    private static void runQ12() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<StrategyEvent> strategyEvents = JsonLineSource.read(
                "data/strategy_events.jsonl",
                StrategyEvent.class);
        DataStream<PitReactionCount> counts = Q12PitReactionWindowCount.build(
                executionEnvironment,
                strategyEvents);
        printResults("Q12", counts);
    }

    private static void runBonus() throws Exception {
        StreamExecutionEnvironment executionEnvironment = createEnvironment();
        List<LapEvent> lapEvents = JsonLineSource.read("data/lap_events.jsonl", LapEvent.class);
        DataStream<RivalContext> contexts = BonusRivalContextWindow.build(executionEnvironment, lapEvents);
        printResults("Bonus", contexts);
    }

    private static StreamExecutionEnvironment createEnvironment() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironment(1);
        executionEnvironment.setParallelism(1);
        return executionEnvironment;
    }

    private static <T> void printResults(String prefix, DataStream<T> outputStream) throws Exception {
        try (CloseableIterator<T> resultIterator = outputStream.executeAndCollect()) {
            while (resultIterator.hasNext()) {
                System.out.println(prefix + " " + resultIterator.next());
            }
        }
    }

    private static void printUsage() {
        System.out.println("Choose one exercise: q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, bonus, or all.");
        System.out.println("Example:");
        System.out.println("  mvn -q compile exec:java "
                + "-Dexec.mainClass=it.polimi.sda.flinkf1.F1FlinkCoursewareJob "
                + "-Dexec.args=q2");
    }

    private static void runInForkedJvm(String[] args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(javaExecutable());
        command.add("-D" + FORK_PROPERTY + "=true");
        command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=error");
        command.add("-cp");
        command.add(String.join(File.pathSeparator, classpathEntries()));
        command.add(F1FlinkCoursewareJob.class.getName());
        command.addAll(Arrays.asList(args));

        Process process = new ProcessBuilder(command)
                .inheritIO()
                .start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("Forked Flink job failed with exit code " + exitCode);
        }
    }

    private static String javaExecutable() {
        String javaBinary = isWindows() ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", javaBinary).toString();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static Set<String> classpathEntries() {
        Set<String> entries = new LinkedHashSet<>();
        addJavaClassPath(entries);
        addClassLoaderUrls(entries, F1FlinkCoursewareJob.class.getClassLoader());
        return entries;
    }

    private static void addJavaClassPath(Set<String> entries) {
        String javaClassPath = System.getProperty("java.class.path", "");
        if (!javaClassPath.isBlank()) {
            entries.addAll(Arrays.asList(javaClassPath.split(File.pathSeparator)));
        }
    }

    private static void addClassLoaderUrls(Set<String> entries, ClassLoader classLoader) {
        ClassLoader currentClassLoader = classLoader;
        while (currentClassLoader != null) {
            if (currentClassLoader instanceof URLClassLoader urlClassLoader) {
                for (URL url : urlClassLoader.getURLs()) {
                    entries.add(toClasspathEntry(url));
                }
            }
            currentClassLoader = currentClassLoader.getParent();
        }
    }

    private static String toClasspathEntry(URL url) {
        try {
            return Path.of(url.toURI()).toString();
        } catch (Exception ignored) {
            return url.getPath();
        }
    }
}
