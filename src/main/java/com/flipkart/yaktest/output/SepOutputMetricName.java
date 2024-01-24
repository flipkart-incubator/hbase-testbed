package com.flipkart.yaktest.output;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SepOutputMetricName implements OutputMetricName {
    @JsonProperty("dataMismatchRH")
    DATA_MISMATCH_RH("dataMismatchRH"),

    @JsonProperty("dataMismatchWH")
    DATA_MISMATCH_WH("dataMismatchWH"),

    @JsonProperty("connectionMismatch")
    CONNECTION_MISMATCH("connectionMismatch"),

    @JsonProperty("dataLoss")
    DATA_LOSS("dataLoss"),

    @JsonProperty("ordering")
    ORDERING("ordering"),

    @JsonProperty("repetition")
    REPETITION("repetition");

    private String sepOutputMetric;

    SepOutputMetricName(String name) {
        this.sepOutputMetric = name;
    }
}
