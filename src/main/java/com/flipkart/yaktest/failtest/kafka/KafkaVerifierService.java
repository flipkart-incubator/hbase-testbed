package com.flipkart.yaktest.failtest.kafka;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.failtest.AbstractSepVerifierService;
import com.flipkart.yaktest.interruption.models.KafkaConfigKey;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaVerifierService extends AbstractSepVerifierService {

    private static Logger logger = LoggerFactory.getLogger(KafkaVerifierService.class);
    private static final int KAFKA_TOPIC_PARTITIONS = 32;
    private ExecutorService kafkaExec = null;
    private KafkaHelper kafkaHelper;
    private AtomicBoolean runFlag = new AtomicBoolean(true);


    @Override
    public void consume() {
        kafkaExec = Executors.newFixedThreadPool(KAFKA_TOPIC_PARTITIONS * 2);
        Map<KafkaConfigKey, List<String>> kafkaConfig = Config.getInstance().getKafkaConfig();
        logger.info("received kafka config {}", kafkaConfig);
        kafkaHelper = new KafkaHelper(
                kafkaConfig.get(KafkaConfigKey.ZK_PATHS).stream().map(path -> buildKafkaConfig(kafkaConfig.get(KafkaConfigKey.ZOOKEEPER), path, KAFKA_TOPIC_PARTITIONS))
                        .collect(Collectors.toList()).toArray(new KafkaConfig[0]));

        for (List<KafkaStream<byte[], byte[]>> streams : kafkaHelper.getStreams()) {
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                kafkaExec.execute(new KafkaVerifyRunner(stream, sepAllVersionsMap, runFlag, KAFKA_TOPIC_PARTITIONS));
            }
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        if (kafkaExec != null) {
            kafkaExec.shutdown();
            boolean done = kafkaExec.awaitTermination(3, TimeUnit.SECONDS);
            if (!done) kafkaExec.shutdownNow();
            kafkaHelper.shutDown();
        }
    }

    private static KafkaConfig buildKafkaConfig(List<String> zkHosts, String zkPath, int partitions) {
        StringJoiner stringJoiner = new StringJoiner(",", "", "/" + zkPath);
        zkHosts.forEach(host -> stringJoiner.add(host + ":2181"));
        String zkConnectionString = stringJoiner.toString();
        KafkaConfig config = new KafkaConfig();
        config.eventConsumer = new Properties();
        config.eventConsumer.put("group.id", "yaksepgroup");
        config.eventConsumer.put("zookeeper.session.timeout.ms", "1000");
        config.eventConsumer.put("zookeeper.connect", zkConnectionString);
        config.eventConsumer.put("zookeeper.sync.time.ms", "200");
        config.eventConsumer.put("auto.commit.interval.ms", "30000");
        config.topic = Config.getInstance().getKafkaConfig().get(KafkaConfigKey.TOPICS).get(0);
        config.partitions = partitions;
        return config;
    }
}
