package com.flipkart.yaktest.interruption.models;

public enum YakComponent {

    JOURNAL_NODE("journalnode"),
    NAME_NODE("namenode"),
    ZKFC("zkfc"),
    DATA_NODE("datanode"),
    ZOOKEEPER("zookeeper"),
    MASTER("master"),
    REGION_SERVER("regionserver");

    private String value;

    YakComponent(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static YakComponent getByValue(String value) {
        for (YakComponent yakComponent : YakComponent.values()) {
            if (yakComponent.getValue().equals(value)) {
                return yakComponent;
            }
        }
        return null;
    }
}
