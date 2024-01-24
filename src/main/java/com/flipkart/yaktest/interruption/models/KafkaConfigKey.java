package com.flipkart.yaktest.interruption.models;

public enum KafkaConfigKey {

    ZOOKEEPER("zookeeper"),
    BROKERS("brokers"),
    TOPICS("topics"),
    ZK_PATHS("zk_paths");

    private String value;

    KafkaConfigKey(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static KafkaConfigKey getByValue(String value) {
        for (KafkaConfigKey kafkaConfigKey : KafkaConfigKey.values()) {
            if (kafkaConfigKey.getValue().equals(value)) {
                return kafkaConfigKey;
            }
        }
        return null;
    }

}
