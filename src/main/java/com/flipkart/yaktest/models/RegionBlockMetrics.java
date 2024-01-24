package com.flipkart.yaktest.models;

public class RegionBlockMetrics {

    private long primaryBlocks;
    private long replicaBlocks;
    private String primaryRegionBytes;
    private long primaryBytesPercent;
    private String replicaRegionBytes;
    private String totalBytes;
    private long numPrimaryRegions;
    private long numReplicaRegions;

    public long getPrimaryBlocks() {
        return primaryBlocks;
    }

    public long getReplicaBlocks() {
        return replicaBlocks;
    }

    public String getPrimaryRegionBytes() {
        return primaryRegionBytes;
    }

    public long getPrimaryBytesPercent() {
        return primaryBytesPercent;
    }

    public String getReplicaRegionBytes() {
        return replicaRegionBytes;
    }

    public String getTotalBytes() {
        return totalBytes;
    }

    public long getNumPrimaryRegions() {
        return numPrimaryRegions;
    }

    public long getNumReplicaRegions() {
        return numReplicaRegions;
    }
}
