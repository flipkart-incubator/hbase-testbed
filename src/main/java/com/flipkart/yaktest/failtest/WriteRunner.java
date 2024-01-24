package com.flipkart.yaktest.failtest;

import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yaktest.failtest.dao.Store;
import com.flipkart.yaktest.failtest.exception.DataMismatchException;
import com.flipkart.yaktest.failtest.utils.LogHelper;
import com.flipkart.yaktest.failtest.utils.RandomUtil;
import com.flipkart.yaktest.output.HbaseOutputMetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.flipkart.yaktest.output.OutputStatusUtil.incHbaseMetricFailureCount;

public class WriteRunner implements Callable<Map<String, Integer>> {

    private static Logger logger = LoggerFactory.getLogger(WriteRunner.class);
    private final String[] keys;
    private final int[] nextVersion;
    private final int runSize;
    private final int dataSize;
    private final Store store;
    private AtomicInteger writeMessageCount;
    private final int runId;
    private Random random;
    private static final int TEN_SECONDS = 10000;
    private final Optional<String> routeKey;
    private boolean multiVersionFlag = false;

    private boolean dualDCPutFlag = false;
    private int totalVersions = 5;

    public WriteRunner(int dataSize, int runSize, Store store, AtomicInteger writeMessageCount, int runId, String routeKey) {
        this.keys = new String[dataSize];
        this.nextVersion = new int[dataSize];
        this.runSize = runSize;
        this.dataSize = dataSize;
        this.runId = runId;
        this.store = store;
        this.routeKey = Optional.ofNullable(routeKey);
        for (int i = 0; i < dataSize; i++) {
            String key = RandomUtil.INSTANCE.randomKey();
            this.keys[i] = key;
            this.nextVersion[i] = 1;
        }
        this.writeMessageCount = writeMessageCount;
        this.random = new Random();
    }

    public WriteRunner(int dataSize, int runSize, Store store, AtomicInteger writeMessageCount, int runId, String routeKey, boolean dualDCPutFlag) {
        this(dataSize, runSize, store, writeMessageCount, runId, routeKey);
        this.dualDCPutFlag = dualDCPutFlag;
    }

    public WriteRunner(int dataSize, int runSize, Store store, AtomicInteger writeMessageCount, int runId, String routeKey, boolean multiVersionFlag, int totalVersions) {
        this(dataSize, runSize, store, writeMessageCount, runId, routeKey);
        this.multiVersionFlag = multiVersionFlag;
        this.totalVersions = totalVersions;
    }

    @Override
    public Map<String, Integer> call() throws InterruptedException {
        String context = " runId: " + runId + " ThreadId: " + Thread.currentThread().getName();
        logger.debug("Started WriteRunner for {}", context);

        for (int i = 0; i < runSize; i++) {
            final int index = Math.abs((random.nextInt() * 100) % dataSize); //pick random key
            String key = keys[index];
            String enrichKey = LogHelper.INSTANCE.hbaseKey(key);
            int version = nextVersion[index];
            if (dualDCPutFlag || multiVersionFlag) {
                if (version != 1) {
                    continue;
                }
            }
            if (multiVersionFlag) {
                for (int repetition = 0; repetition < totalVersions; repetition++) {
                    checkPutStore(key, version + repetition, context, index, i, enrichKey, Optional.empty());
                    TimeUnit.MILLISECONDS.sleep(2);
                }
            } else {
                checkPutStore(key, version, context, index, i, enrichKey, Optional.empty());
                if (dualDCPutFlag) {
                    TimeUnit.MILLISECONDS.sleep(1500);
                    checkPutStore(key, nextVersion[index], context, index, i, enrichKey, routeKey);
                }
            }
        }
        Map<String, Integer> output = new HashMap<>(2 * dataSize);
        for (int i = 0; i < dataSize; i++) {
            if (nextVersion[i] == 1) {
                continue; //these were not written
            }
            output.put(keys[i], nextVersion[i] - 1);
        }
        logger.debug("Completed WriteRunner for {}", context);
        return output;
    }

    private void checkPutStore(String key, int version, String context, int index, int i, String enrichKey, Optional<String> routeKey) {
        String localContext = " key:" + enrichKey + " version:" + version + context;

        logger.debug("putting {} run: {}", localContext, i);
        boolean updated = false;
        boolean recoveredAfterCausingException = false;
        try {
            updated = store.checkPut(key, version, version - 1, routeKey);
        } catch (Exception e) {
            logger.error("Check put failure for Exception {}", localContext, e);
            recoveredAfterCausingException = true;
        }
        int retry = 6;
        while (!Thread.currentThread().isInterrupted()) {
            retry -= 1;
            if (!updated) {
                try {
                    logger.debug("Verifying get with {} retry: {} index: {}", context, retry, i);
                    store.verifyGet(key, version, routeKey, false);
                    updated = true;
                    break;
                } catch (DataMismatchException e) {
                    incHbaseMetricFailureCount(HbaseOutputMetricName.CHECK_PUT_FAIL, true);
                    logger.warn("Check put verifier failed with data/version mismatch for {}", localContext, e);
                    if (retry < 0) {
                        break;
                    }
                } catch (StoreDataNotFoundException e) {
                    incHbaseMetricFailureCount(HbaseOutputMetricName.CHECK_PUT_FAIL, true);
                    logger.warn("Check put verifier failed with key not found for {}", localContext, e);
                    if (retry < 0) {
                        break;
                    }
                } catch (Exception e) {
                    // Here we are not sure if data is written or not. So, cannot proceed here because, it will lead to test
                    // cases failures. We must ensure client has 100% understanding of a specific data is present or not, to
                    // validate if there are issue with hbase server or not interms of data loss or mismatch
                    logger.error("Check put verifier failure for {} Exception retry: {} verifier after a delay of millis: {}", localContext, retry, TEN_SECONDS, e);
                }
            } else {
                logger.debug("put done for {} run: {}", localContext, i);
                break;
            }
            try {
                Thread.sleep(TEN_SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Failed to sleep while check put verifier for {}", localContext, e);
                Thread.currentThread().interrupt();
                logger.warn("Interrupt received, will close writing to store");
            }
        }

        if (updated) {
            if (recoveredAfterCausingException) {
                logger.info("Check put succeeded after causing exception {} ", localContext);
            }
            writeMessageCount.incrementAndGet();
            nextVersion[index]++;
        } else {
            logger.error("Failed to get any info for {}", localContext);
        }
    }

}
