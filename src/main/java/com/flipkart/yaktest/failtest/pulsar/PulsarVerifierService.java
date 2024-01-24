package com.flipkart.yaktest.failtest.pulsar;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.failtest.AbstractSepVerifierService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PulsarVerifierService extends AbstractSepVerifierService {

    private static Logger logger = LoggerFactory.getLogger(PulsarVerifierService.class);
    private ExecutorService executorService;
    private AtomicBoolean runFlag = new AtomicBoolean(true);

    @Override
    public void consume() {
        try {
            PulsarConsumerFactory.init();
            PulsarConfig pulsarConfig = Config.getInstance().getPulsarConfig();
            executorService = Executors.newFixedThreadPool(pulsarConfig.getNumOfConsumer());
            for (String topic : pulsarConfig.getTopics()) {
                logger.info("submitting verifier job for topic {}", topic);
                Consumer<byte[]> consumer = PulsarConsumerFactory.getConsumer(topic);
                executorService.execute(new PulsarConsumerRunner(sepAllVersionsMap, consumer, runFlag));
            }
        } catch (PulsarClientException e) {
            logger.error("could not initialise pulsar consumers", e);
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        if (executorService != null && !executorService.isTerminated()) {
            executorService.awaitTermination(10L, TimeUnit.SECONDS);
            executorService.shutdown();
        }
        PulsarConsumerFactory.shutdown();
    }
}
