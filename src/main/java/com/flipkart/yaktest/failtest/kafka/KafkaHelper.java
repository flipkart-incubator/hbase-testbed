package com.flipkart.yaktest.failtest.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaHelper {
    private List<ConsumerConnector> consumers;
    private List<KafkaConfig> configs;

    public KafkaHelper(KafkaConfig... configs) {
        this.configs = Arrays.asList(configs);
        consumers = this.configs.stream().map(config -> kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(config.eventConsumer)))
                .collect(Collectors.toList());
        consumers.forEach(ConsumerConnector::commitOffsets); //commit offsets held before starting this so that it starts consuming from the latest offset
    }

    public List<List<KafkaStream<byte[], byte[]>>> getStreams() {

        List<List<KafkaStream<byte[], byte[]>>> streams = new ArrayList<>();
        for (int i = 0; i < consumers.size(); i++) {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(configs.get(i).topic, configs.get(i).partitions);
            streams.add(consumers.get(i).createMessageStreams(topicCountMap).get(configs.get(i).topic));
        }
        return streams;
    }

    public void shutDown() {
        consumers.forEach(consumer -> {
            if (null != consumer) {
                consumer.shutdown();
            }
        });
    }
}
