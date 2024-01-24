package com.flipkart.yaktest.failtest.kafka;

import java.util.Properties;

public class KafkaConfig {

    public Properties eventConsumer;
    public Properties sidelineEventConsumer;
    public Properties sidelineEventProducer;
    public String topic;
    public int partitions;
    public String sidelineTopic;
    public int sidelinePartitions;
}
