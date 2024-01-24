package com.flipkart.yaktest.output;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.concurrent.atomic.AtomicInteger;

public class OutputMetricResult {

    private OutputMetricName name;
    private AtomicInteger failureCount = new AtomicInteger(0);
    private String description;
    private boolean result;
    private Status status = Status.PASSED;

    public OutputMetricResult() {

    }

    public OutputMetricResult(OutputMetricName name, boolean result) {
        this.name = name;
        this.result = result;
    }

    public void setResult(Boolean result) {
        this.result = result;
    }

    @JsonDeserialize(using = OutputMetricNameDeserializer.class)
    public OutputMetricName getName() {
        return name;
    }

    public boolean getResult() {
        return result;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setName(OutputMetricName name) {
        this.name = name;
    }

    public AtomicInteger getFailureCount() {
        return failureCount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "OutputMetricResult{" + "name=" + name + ", result=" + result + ", status=" + status + '}';
    }
}
