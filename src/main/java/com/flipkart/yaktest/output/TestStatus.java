package com.flipkart.yaktest.output;

import java.util.SortedMap;
import java.util.TreeMap;

import static com.flipkart.yaktest.output.Status.PASSED;

public class TestStatus {

    private SortedMap<HbaseOutputMetricName, OutputMetricResult> hbaseMetrices = new TreeMap<>();
    private SortedMap<SepOutputMetricName, OutputMetricResult> sepMetrices = new TreeMap<>();
    private Status overallStatus = PASSED;

    TestStatus() {
        hbaseMetrices.put(HbaseOutputMetricName.DATA_LOSS, new OutputMetricResult(HbaseOutputMetricName.DATA_LOSS, false));
        hbaseMetrices.put(HbaseOutputMetricName.DATA_MISMATCH_RH, new OutputMetricResult(HbaseOutputMetricName.DATA_MISMATCH_RH, false));
        hbaseMetrices.put(HbaseOutputMetricName.DATA_MISMATCH_WH, new OutputMetricResult(HbaseOutputMetricName.DATA_MISMATCH_WH, false));
        hbaseMetrices.put(HbaseOutputMetricName.GET_FAIL, new OutputMetricResult(HbaseOutputMetricName.GET_FAIL, false));
        hbaseMetrices.put(HbaseOutputMetricName.CHECK_PUT_EXCEPTION, new OutputMetricResult(HbaseOutputMetricName.CHECK_PUT_EXCEPTION, false));
        hbaseMetrices.put(HbaseOutputMetricName.CHECK_PUT_FAIL, new OutputMetricResult(HbaseOutputMetricName.CHECK_PUT_FAIL, false));
        hbaseMetrices.put(HbaseOutputMetricName.WAL_ISOLATED, new OutputMetricResult(HbaseOutputMetricName.WAL_ISOLATED, true));
        hbaseMetrices.put(HbaseOutputMetricName.HBLOCK_ISOLATED, new OutputMetricResult(HbaseOutputMetricName.HBLOCK_ISOLATED, true));
        hbaseMetrices.put(HbaseOutputMetricName.ALL_REGIONS_OPEN, new OutputMetricResult(HbaseOutputMetricName.ALL_REGIONS_OPEN, true));
        hbaseMetrices.put(HbaseOutputMetricName.FAILURES_AFTER_TEST, new OutputMetricResult(HbaseOutputMetricName.FAILURES_AFTER_TEST, false));
        hbaseMetrices.put(HbaseOutputMetricName.INCONSISTENCIES, new OutputMetricResult(HbaseOutputMetricName.INCONSISTENCIES, false));
        hbaseMetrices.put(HbaseOutputMetricName.REGIONS_ARE_RACK_SPREAD, new OutputMetricResult(HbaseOutputMetricName.REGIONS_ARE_RACK_SPREAD, false));

        sepMetrices.put(SepOutputMetricName.DATA_LOSS, new OutputMetricResult(SepOutputMetricName.DATA_LOSS, false));
        sepMetrices.put(SepOutputMetricName.ORDERING, new OutputMetricResult(SepOutputMetricName.ORDERING, true));
        sepMetrices.put(SepOutputMetricName.REPETITION, new OutputMetricResult(SepOutputMetricName.REPETITION, false));
        sepMetrices.put(SepOutputMetricName.DATA_MISMATCH_RH, new OutputMetricResult(SepOutputMetricName.DATA_MISMATCH_RH, false));
        sepMetrices.put(SepOutputMetricName.DATA_MISMATCH_WH, new OutputMetricResult(SepOutputMetricName.DATA_MISMATCH_WH, false));
        sepMetrices.put(SepOutputMetricName.CONNECTION_MISMATCH, new OutputMetricResult(SepOutputMetricName.CONNECTION_MISMATCH, false));

    }

    public SortedMap<HbaseOutputMetricName, OutputMetricResult> getHbaseMetrices() {
        return hbaseMetrices;
    }

    public SortedMap<SepOutputMetricName, OutputMetricResult> getSepMetrices() {
        return sepMetrices;
    }

    public Status getOverallStatus() {
        return overallStatus;
    }

    public void setOverallStatus(Status overallStatus) {
        this.overallStatus = overallStatus;
    }

    @Override
    public String toString() {
        return "TestStatus{" + "hbaseMetrices=" + hbaseMetrices + ", kafkaMetrices=" + sepMetrices + ", overallStatus=" + overallStatus + '}';
    }
}
