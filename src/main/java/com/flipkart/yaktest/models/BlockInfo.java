package com.flipkart.yaktest.models;

import java.util.ArrayList;
import java.util.List;

public class BlockInfo {
    private String blockName;
    private String bytes;
    private int replicationFactor;
    private String primaryHost;
    private List<String> secondaryHosts = new ArrayList<>();

    @Override
    public String toString() {
        return "BlockInfo{" + "blockName='" + blockName + '\'' + ", bytes=" + bytes + ", replicationFactor=" + replicationFactor + ", primaryHost='"
                + primaryHost + '\'' + ", secondaryHosts=" + secondaryHosts + '}';
    }

    public String getBlockName() {
        return blockName;
    }

    public void setBlockName(String blockName) {
        this.blockName = blockName;
    }

    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getPrimaryHost() {
        return primaryHost;
    }

    public void setPrimaryHost(String primaryHost) {
        this.primaryHost = primaryHost;
    }

    public List<String> getSecondaryHosts() {
        return secondaryHosts;
    }

    public void setSecondaryHosts(List<String> secondaryHosts) {
        this.secondaryHosts = secondaryHosts;
    }
}
