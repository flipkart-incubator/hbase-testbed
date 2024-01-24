package com.flipkart.yaktest.failtest.pulsar;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sep.shade.com.flipkart.yak.sep.SerDe;
import sep.shade.com.flipkart.yak.sep.SerDeException;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PulsarConsumerRunner implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PulsarConsumerRunner.class);
    private final ConcurrentMap<String, List<Integer>> messageVersionMap;
    private final Consumer<byte[]> consumer;
    private final AtomicBoolean runFlag;
    private final String KEY_DELIMETER = "-";

    public PulsarConsumerRunner(ConcurrentMap<String, List<Integer>> messageVersionMap, Consumer<byte[]> consumer,
                                AtomicBoolean runFlag) {
        this.messageVersionMap = messageVersionMap;
        this.consumer = consumer;
        this.runFlag = runFlag;
    }

    @Override
    public void run() {
        logger.info("starting consumer with topic {} run Signal {}", this.consumer.getTopic(), this.runFlag.get());
        while (this.runFlag.get() && this.consumer.isConnected()) {
            try {
                Messages<byte[]> messages = consumer.batchReceive();
                logger.debug("topic {} consumer name {} connected: {}", consumer.getTopic(), consumer.getConsumerName(), consumer.isConnected());
                if (messages != null && messages.size() > 0) {
                    Message<byte[]> lastMessage = null;
                    for (Message<byte[]> message : messages) {
                        SepMessageProto.SepMessage msg = SerDe.DESERIALZIER.execute(message.getData());
                        String rowKey = new String(msg.getRow().toByteArray());
                        String key = rowKey.split(KEY_DELIMETER)[1];
                        byte[] bytesMsg = msg.getValue().toByteArray();
                        int val = Integer.parseInt(Bytes.toString(bytesMsg));
                        messageVersionMap.putIfAbsent(key, new ArrayList<>());
                        messageVersionMap.get(key).add(val);
                        lastMessage = message;
                        consumer.acknowledge(lastMessage);
                    }
                    if (lastMessage != null) {
                        logger.info("ack-ed {} messages", messages.size());
                    }
                }
            } catch (PulsarClientException e) {
                logger.error("Batch received failed {}", e.getMessage(), e);
            } catch (SerDeException e) {
                logger.error("Deserialization failed {}", e.getMessage(), e);
            }
        }
    }
}
