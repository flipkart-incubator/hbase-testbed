package com.flipkart.yaktest.failtest.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sep.shade.com.flipkart.yak.sep.SerDe;
import sep.shade.com.flipkart.yak.sep.SerDeException;
import sep.shade.com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaVerifyRunner implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaVerifyRunner.class);
    private final KafkaStream<byte[], byte[]> stream;
    private final ConcurrentMap<String, List<Integer>> kafkaAllVersionsMap;
    private final AtomicBoolean runFlag;

    public KafkaVerifyRunner(KafkaStream<byte[], byte[]> stream, ConcurrentMap<String, List<Integer>> kafkaAllVersionsMap, AtomicBoolean runFlag,
                             int partitions) {
        this.stream = stream;
        this.kafkaAllVersionsMap = kafkaAllVersionsMap;
        this.runFlag = runFlag;
    }

    @Override
    public void run() {

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (runFlag.get() && it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> event = it.next();
            try {
                int partition = event.partition();
                logger.debug("Thread " + Thread.currentThread().getId() + " for partition " + partition + " offset " + event.offset());
                try {
                    SepMessage message = SerDe.DESERIALZIER.execute(event.message());
                    String rowKey = new String(message.getRow().toByteArray());
                    String key = rowKey.split("-")[1];
                    byte[] bytesMsg = message.getValue().toByteArray();
                    int val = Integer.parseInt(Bytes.toString(bytesMsg));
                    logger.debug("kafka got " + rowKey + "_" + val);

                    kafkaAllVersionsMap.putIfAbsent(key, new ArrayList<>());
                    kafkaAllVersionsMap.get(key).add(val);
                } catch (SerDeException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("Failed for Thread " + Thread.currentThread(), e);
                e.printStackTrace();
            }
        }
    }
}
