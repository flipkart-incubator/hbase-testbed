package com.flipkart.yaktest.models;

public class BlockStatus {

    private long okBlockCount = 0;
    private long totalBlockCount = 0;
    private String description = "";

    public long getOkBlockCount() {
        return okBlockCount;
    }

    public void setOkBlockCount(long okBlockCount) {
        this.okBlockCount = okBlockCount;
    }

    public long getTotalBlockCount() {
        return totalBlockCount;
    }

    public void setTotalBlockCount(long totalBlockCount) {
        this.totalBlockCount = totalBlockCount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
