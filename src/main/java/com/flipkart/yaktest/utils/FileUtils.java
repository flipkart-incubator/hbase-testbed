package com.flipkart.yaktest.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.flipkart.yaktest.output.TestOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;

public class FileUtils {

    private FileUtils() {
    }

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static String logPath = "/var/log/flipkart/yak-testbed/";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final Set<PosixFilePermission> worldWritable = PosixFilePermissions.fromString("rw-rw-rw-");
    private static FileAttribute<?> worldWritableAttributes = PosixFilePermissions.asFileAttribute(worldWritable);

    static {
        SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss");
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(df);
    }

    public static String getLogPath() {
        return logPath;
    }

    public static void appendJobIdToLogsDir(String jobId) {
        logPath = logPath + jobId + "/";
    }

    public static void writeOutputToFile(TestOutput testOutput) {
        try {
            Path path = Paths.get(logPath + "output.json");
            Files.createDirectories(path.getParent(), worldWritableAttributes);
            objectMapper.writeValue(path.toFile(), testOutput);

        } catch (IOException e) {
            logger.error("error writing output to file", e);
        }
    }

    public static void writeReportToFile(List<TestOutput> report) {

        try {
            Path path = Paths.get(logPath + "aggregated_report.json");
            objectMapper.writeValue(path.toFile(), report);

        } catch (IOException e) {
            logger.error("error reading outputs from file {}", e.getMessage(), e);
        }
    }

    public static List<TestOutput> readReportFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return objectMapper.readValue(path.toFile(), new TypeReference<List<TestOutput>>() {
        });
    }

    public static void createLogDirectory() throws IOException {
        Path path = Paths.get(logPath);
        Files.createDirectories(path);
    }
}
