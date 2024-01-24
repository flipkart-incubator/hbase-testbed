package com.flipkart.yaktest.output;

public class OutputStatusUtil {

    private OutputStatusUtil() {
    }

    private static TestStatus testStatus = TestOutput.INSTANCE.getTestStatus();

    public static void incHbaseMetricFailureCount(OutputMetricName outputMetricName, boolean result) {
        OutputMetricResult outputMetricResult = testStatus.getHbaseMetrices().get(outputMetricName);
        outputMetricResult.setResult(result);
        outputMetricResult.getFailureCount().incrementAndGet();
    }

    public static void incSepMetricFailureCount(OutputMetricName outputMetricName, boolean result) {
        OutputMetricResult outputMetricResult = testStatus.getSepMetrices().get(outputMetricName);
        outputMetricResult.setResult(result);
        outputMetricResult.getFailureCount().incrementAndGet();
    }

    public static void incSepMetricFailureCount(OutputMetricName outputMetricName, int incrementedValue, boolean result) {
        OutputMetricResult outputMetricResult = testStatus.getSepMetrices().get(outputMetricName);
        outputMetricResult.setResult(result);
        outputMetricResult.getFailureCount().addAndGet(incrementedValue);
    }
}
