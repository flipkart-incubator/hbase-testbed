package com.flipkart.yaktest.interruption.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum InterruptionName {

    @JsonProperty("killRegionServer")
    KILL_REGION_SERVER("killRegionServer"),

    @JsonProperty("stopRegionServer")
    STOP_REGION_SERVER("stopRegionServer"),

    @JsonProperty("killDataNode")
    KILL_DATA_NODE("killDataNode"),

    @JsonProperty("stopDataNode")
    STOP_DATA_NODE("stopDataNode"),

    @JsonProperty("killZookeeper")
    KILL_ZOOKEEPER("killZookeeper"),

    @JsonProperty("stopZookeeper")
    STOP_ZOOKEEPER("stopZookeeper"),

    @JsonProperty("killJournalNode")
    KILL_JOURNAL_NODE("killJournalNode"),

    @JsonProperty("stopJournalNode")
    STOP_JOURNAL_NODE("stopJournalNode"),

    @JsonProperty("killHMaster")
    KILL_H_MASTER("killHMaster"),

    @JsonProperty("stopHMaster")
    STOP_H_MASTER("stopHMaster"),

    @JsonProperty("splitRegions")
    SPLIT_REGIONS("splitRegions"),

    @JsonProperty("mergeRegions")
    MERGE_REGIONS("mergeRegions"),

    @JsonProperty("masterNamenodeAcrossNetwork")
    MASTER_NAMENODE_ACROSS_NETWORK("masterNamenodeAcrossNetwork"),

    @JsonProperty("networkPartitionDataNode")
    NETWORK_PARTITION_DATA_NODE("networkPartitionDataNode"),

    @JsonProperty("networkPartitionMaster")
    NETWORK_PARTITION_MASTER("networkPartitionMaster"),

    @JsonProperty("networkPartitionNameNode")
    NETWORK_PARTITION_NAME_NODE("networkPartitionNameNode"),

    @JsonProperty("networkPartitionZK")
    NETWORK_PARTITION_ZK("networkPartitionZK"),

    @JsonProperty("networkPartitionJN")
    NETWORK_PARTITION_JN("networkPartitionJN"),

    @JsonProperty("blank")
    BLANK("blank");

    private String name;

    public String getName() {
        return name;
    }

    InterruptionName(String name) {
        this.name = name;
    }
}
