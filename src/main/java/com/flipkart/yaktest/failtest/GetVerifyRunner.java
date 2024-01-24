package com.flipkart.yaktest.failtest;

import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yaktest.failtest.dao.Store;
import com.flipkart.yaktest.failtest.exception.DataMismatchException;
import com.flipkart.yaktest.failtest.utils.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.flipkart.yaktest.output.HbaseOutputMetricName.*;
import static com.flipkart.yaktest.output.OutputStatusUtil.incHbaseMetricFailureCount;

public class GetVerifyRunner implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(GetVerifyRunner.class);
    private final CountDownLatch latch;
    private final Map<String, Integer> idVersionMap;
    private final Store store;
    private final Optional<String> routeKey;
    private final boolean queryForBackup;
    private boolean multiVersionFlag = false;
    private boolean dualDCPutFlag = false;
    private int totalVersions = 5;

    public GetVerifyRunner(CountDownLatch latch, Map<String, Integer> idVersionMap, Store store, String routeKey,
                           boolean queryForBackup) {
        this.latch = latch;
        this.idVersionMap = idVersionMap;
        this.store = store;
        this.routeKey = Optional.ofNullable(routeKey);
        this.queryForBackup = queryForBackup;
    }

    public GetVerifyRunner(CountDownLatch latch, Map<String, Integer> idVersionMap, Store store, String routeKey,
                           boolean queryForBackup, boolean multiVersionFlag, int totalVersions) {
        this(latch, idVersionMap, store, routeKey, queryForBackup);
        this.multiVersionFlag = multiVersionFlag;
        this.totalVersions = totalVersions;
    }

    public GetVerifyRunner(CountDownLatch latch, Map<String, Integer> idVersionMap, Store store, String routeKey,
                           boolean queryForBackup, boolean dualDCPutFlag) {
        this(latch, idVersionMap, store, routeKey, queryForBackup);
        this.dualDCPutFlag = dualDCPutFlag;
    }

    @Override
    public void run() {
        routeKey.ifPresent(s -> logger.debug("Read is being done from: {} size: {}", s, idVersionMap.size()));
        for (Map.Entry<String, Integer> rowVersion : idVersionMap.entrySet()) {
            try {
                if (multiVersionFlag) {
                    store.verifyGet(rowVersion.getKey(), rowVersion.getValue(), routeKey, queryForBackup, totalVersions);
                } else {
                    store.verifyGet(rowVersion.getKey(), rowVersion.getValue(), routeKey, queryForBackup);
                    if (dualDCPutFlag) {
                        store.verifyGet(rowVersion.getKey(), rowVersion.getValue(), Optional.empty(), queryForBackup);
                    }
                }
                logger.debug("Verify get is successful: {}", rowVersion);
            } catch (DataMismatchException e) {
                if (e.getType().equals(DataMismatchException.Type.READ_VERSION_HIGHER)) {
                    incHbaseMetricFailureCount(DATA_MISMATCH_RH, true);
                } else if (e.getType().equals(DataMismatchException.Type.WRITE_VERSION_HIGHER)) {
                    incHbaseMetricFailureCount(DATA_MISMATCH_WH, true);
                }
                logger.error("Data Mismatch", e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StoreDataNotFoundException) {
                    incHbaseMetricFailureCount(DATA_LOSS, true);
                    logger.error("Failed in verifyGet call FOR {}  version {} ", LogHelper.INSTANCE.hbaseKey(rowVersion.getKey()), rowVersion.getValue(),
                            e);
                } else {
                    incHbaseMetricFailureCount(GET_FAIL, true);
                    logger.error("Failed in verifyGet call FOR {}  version {}", LogHelper.INSTANCE.hbaseKey(rowVersion.getKey()), rowVersion.getValue(), e);
                }
            } catch (Exception e) {
                incHbaseMetricFailureCount(GET_FAIL, true);
                logger.error("Failed in verifyGet call FOR {}  version {}", LogHelper.INSTANCE.hbaseKey(rowVersion.getKey()), rowVersion.getValue(), e);
            }
        }
        latch.countDown();
    }
}
