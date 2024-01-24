package com.flipkart.yaktest.failtest;

import com.flipkart.yaktest.output.OutputStatusUtil;
import com.flipkart.yaktest.output.SepOutputMetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SEP -- Side effect processor / Change data capture
 */
public abstract class AbstractSepVerifierService {

    private static Logger logger = LoggerFactory.getLogger(AbstractSepVerifierService.class);
    public final ConcurrentMap<String, List<Integer>> sepAllVersionsMap = new ConcurrentHashMap<>();
    private static final int FIVE_SEC = 5000;
    public static final int ITERATIONS = 6;

    public abstract void consume();

    public abstract void shutdown() throws InterruptedException;

    final void verifyData(final ConcurrentMap<String, Integer> putHighestVersionMap) {
        logger.info("Received all required data from SEP topic. Processing the received data");

        HashMap<String, Integer> getHighestVersionMap = new HashMap<>();
        for (String key : sepAllVersionsMap.keySet()) {
            logger.debug("Processing for key {}, read all versions: {}", key, sepAllVersionsMap.get(key));
            List<Integer> allVersionsList = sepAllVersionsMap.get(key);
            SortedSet<Integer> allVersionsSet = new TreeSet<>(allVersionsList);

            getHighestVersionMap.put(key, allVersionsSet.last());
            if (allVersionsList.size() > allVersionsSet.size()) {
                logger.error("Repetitions for key {} via SEP. Got {}", key, allVersionsList);
                OutputStatusUtil.incSepMetricFailureCount(SepOutputMetricName.REPETITION, true);
            }
            if (!putHighestVersionMap.containsKey(key) || putHighestVersionMap.get(key) < allVersionsSet.last()) {
                logger.error("Data Mismatch Read ahead for key {} via SEP topic. Highest get version {} Highest put version {}",
                        key, allVersionsSet.last(), putHighestVersionMap.getOrDefault(key, 0));
                OutputStatusUtil.incSepMetricFailureCount(SepOutputMetricName.DATA_MISMATCH_RH, true);
            }

            int preVersion = 0;
            boolean outOfOrder = false;
            for (int index = 0; index < allVersionsList.size(); index += 1) {
                int currVersion = allVersionsList.get(index);
                if (currVersion != preVersion + 1) {
                    if (allVersionsList.indexOf(currVersion) >= 0 && allVersionsList.indexOf(currVersion) < index) {
                        preVersion = currVersion - 1;
                    } else {
                        outOfOrder = true;
                    }
                }
                preVersion = currVersion;
            }
            if (outOfOrder) {
                logger.error("OutOfOrder stream of messages for key {} via SEP. All versions received {}", key, allVersionsList);
                OutputStatusUtil.incSepMetricFailureCount(SepOutputMetricName.ORDERING, false);
            }
        }
        for (Map.Entry<String, Integer> key : putHighestVersionMap.entrySet()) {
            logger.debug("Processing for key {}, write highest version: {}", key.getKey(), key.getValue());
            int putHighestVersion = key.getValue();
            if (!getHighestVersionMap.containsKey(key.getKey()) || getHighestVersionMap.get(key.getKey()) < key.getValue()) {
                logger.error("Data Mismatch Write ahead for key {} via SEP. Highest get version {}, Highest put version {}",
                        key, getHighestVersionMap.get(key.getKey()), key.getValue());
                OutputStatusUtil.incSepMetricFailureCount(SepOutputMetricName.DATA_MISMATCH_WH, true);
            }

            boolean dataLoss = false;
            for (int index = 0; index < putHighestVersion; index += 1) {
                if (!sepAllVersionsMap.containsKey(key.getKey()) || !(sepAllVersionsMap.get(key.getKey()).contains(putHighestVersion - index))) {
                    dataLoss = true;
                }
            }

            if (dataLoss) {
                logger.error("Data Loss for key {} via SEP. Highest put version {}, SEP stream {}", key, putHighestVersion,
                        sepAllVersionsMap.get(key.getKey()));
                OutputStatusUtil.incSepMetricFailureCount(SepOutputMetricName.DATA_MISMATCH_RH, true);
            }
        }
    }

    public final void awaitReplicationLag() throws InterruptedException {
        int counter = 0;
        int iteration = 0;
        int prevAllVersionsCount = 0;
        while (true) {
            Thread.sleep(FIVE_SEC);
            counter += 1;
            iteration += 1;
            int currAllVersionsCount = sepAllVersionsMap.values().stream().mapToInt(List::size).sum();
            logger.info("Iteration: {}, currAllVersionCount: {}, prevAllVersionCount: {}", iteration, currAllVersionsCount, prevAllVersionsCount);

            if (currAllVersionsCount > prevAllVersionsCount) {
                prevAllVersionsCount = currAllVersionsCount;
                counter = 0;
            }
            if (counter >= ITERATIONS) { //Three minutes
                logger.info("Waited for {} iterations of 5 seconds each without any new data from SEP topic. \n Stopped waiting for data", ITERATIONS);
                break;
            }
        }
    }
}
