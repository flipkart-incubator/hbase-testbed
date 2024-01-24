package com.flipkart.yaktest;

import com.flipkart.yaktest.output.OutputMetricResult;
import com.flipkart.yaktest.output.Status;
import com.flipkart.yaktest.output.TestOutput;
import com.flipkart.yaktest.output.TestStatus;
import com.flipkart.yaktest.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReportAggregator {

    private static Logger logger = LoggerFactory.getLogger(ReportAggregator.class);

    public static void main(String[] args) {
        ProgramArguments programArguments = new ProgramArguments(args);

        String[] reportFiles = programArguments.switchValues("--reportFiles");
        String jobId = programArguments.switchValue("--jobId");
        List<List<TestOutput>> reports = readAllReports(reportFiles);
        if (reports.isEmpty()) {
            return;
        }
        List<TestOutput> finalReport = reports.get(0);
        updateFinalReports(reports, finalReport);
        FileUtils.appendJobIdToLogsDir(jobId);
        FileUtils.writeReportToFile(finalReport);
    }

    private static void updateFinalReports(List<List<TestOutput>> reports, List<TestOutput> finalReport) {
        reports.forEach(report -> {
            for (int i = 0; i < report.size(); i++) {
                TestStatus testStatus = report.get(i).getTestStatus();
                TestStatus finalTestStatus = finalReport.get(i).getTestStatus();
                if (testStatus.getOverallStatus() == Status.FAILED) {
                    finalTestStatus.setOverallStatus(Status.FAILED);
                }

                testStatus.getSepMetrices().forEach((key, value) -> {
                    if (value.getStatus() == Status.FAILED) {
                        OutputMetricResult outputMetricResult = finalTestStatus.getSepMetrices().get(key);
                        outputMetricResult.setStatus(Status.FAILED);
                        outputMetricResult.setResult(value.getResult());
                        outputMetricResult.getFailureCount().addAndGet(value.getFailureCount().get());
                    }
                });

                testStatus.getHbaseMetrices().forEach((key, value) -> {
                    if (value.getStatus() == Status.FAILED) {
                        OutputMetricResult outputMetricResult = finalTestStatus.getHbaseMetrices().get(key);
                        outputMetricResult.setStatus(Status.FAILED);
                        outputMetricResult.setResult(value.getResult());
                        outputMetricResult.getFailureCount().addAndGet(value.getFailureCount().get());
                    }
                });
            }
        });
    }

    private static List<List<TestOutput>> readAllReports(String[] reportFiles) {
        List<List<TestOutput>> list = new ArrayList<>();
        for (String s : reportFiles) {
            List<TestOutput> testOutputs = null;
            try {
                testOutputs = FileUtils.readReportFromFile(s);
            } catch (IOException e) {
                logger.error("could not read file : {} : {}", s, e.getMessage());
                continue;
            }
            list.add(testOutputs);
        }
        return list;
    }
}