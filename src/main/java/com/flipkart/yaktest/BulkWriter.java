package com.flipkart.yaktest;

import com.flipkart.yaktest.failtest.WriteRunner;
import com.flipkart.yaktest.failtest.dao.Store;
import com.flipkart.yaktest.failtest.utils.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

public class BulkWriter implements Callable<Integer> {

    private static Logger logger = LoggerFactory.getLogger(WriteRunner.class);
    private final int dataSize;
    private final Store store;

    public BulkWriter(int dataSize, Store store) {
        this.dataSize = dataSize;
        this.store = store;
    }

    @Override
    public Integer call() {

        Map<String, Integer> keyVersionMap = new HashMap<>();
        for (int i = 1; i <= dataSize; i++) {
            try {
                keyVersionMap.put(RandomUtil.INSTANCE.randomKey(), 1);
                if (i % 100 == 0 || i == dataSize) {
                    store.batchPut(keyVersionMap, Optional.empty());
                    keyVersionMap.clear();
                }
            } catch (Exception e) {
                logger.error("put failure for {} keys ", keyVersionMap.size(), e);
                keyVersionMap.clear();
            }
        }
        return dataSize;
    }
}
