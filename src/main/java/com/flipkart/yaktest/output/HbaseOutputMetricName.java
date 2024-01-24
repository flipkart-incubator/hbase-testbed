package com.flipkart.yaktest.output;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum HbaseOutputMetricName implements OutputMetricName {
    @JsonProperty("dataMismatchRH")
    DATA_MISMATCH_RH("dataMismatchRH"),

    @JsonProperty("dataMismatchWH")
    DATA_MISMATCH_WH("dataMismatchWH"),

    @JsonProperty("dataLoss")
    DATA_LOSS("dataLoss"),

    @JsonProperty("checkPutFail")
    CHECK_PUT_FAIL("checkPutFail"),

    @JsonProperty("checkPutException")
    CHECK_PUT_EXCEPTION("checkPutException"),

    @JsonProperty("getFail")
    GET_FAIL("getFail"),

    @JsonProperty("inconsistencies")
    INCONSISTENCIES("inconsistencies"),

    @JsonProperty("regionsAreRackSpread")
    REGIONS_ARE_RACK_SPREAD("regionsAreRackSpread"),

    @JsonProperty("walIsolated")
    WAL_ISOLATED("walIsolated"),

    @JsonProperty("hBlockIsolated")
    HBLOCK_ISOLATED("hBlockIsolated"),

    @JsonProperty("allRegionsOpen")
    ALL_REGIONS_OPEN("allRegionsOpen"),

    @JsonProperty("failuresAfterTest")
    FAILURES_AFTER_TEST("failuresAfterTest");

    private String hbaseOutputMetric;

    HbaseOutputMetricName(String name) {
        this.hbaseOutputMetric = name;
    }

}
