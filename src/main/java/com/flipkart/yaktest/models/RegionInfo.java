package com.flipkart.yaktest.models;

public class RegionInfo {

    private String regionName;
    private String startKey;
    private String endKey;
    private String state;

    public RegionInfo(String regionName, String startKey, String endKey) {
        this.regionName = regionName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "RegionInfo{" + "regionName='" + regionName + '\'' + ", startKey='" + startKey + '\'' + ", endKey='" + endKey + '\'' + ", state='" + state
                + '\'' + '}';
    }
}
