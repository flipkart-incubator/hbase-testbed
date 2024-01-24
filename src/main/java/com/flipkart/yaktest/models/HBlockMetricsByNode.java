package com.flipkart.yaktest.models;

import java.util.HashMap;
import java.util.Map;

public class HBlockMetricsByNode {

    private Map<String, RegionBlockMetrics> nodes = new HashMap<>();
    private String bytes;
    private String deviation;
    private String mean;

    public Map<String, RegionBlockMetrics> getNodes() {
        return nodes;
    }

    public String getBytes() {
        return bytes;
    }

    public String getDeviation() {
        return deviation;
    }

    public String getMean() {
        return mean;
    }
}