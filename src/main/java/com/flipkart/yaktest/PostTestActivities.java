package com.flipkart.yaktest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yak.distributor.NoDistribution;
import com.flipkart.yaktest.failtest.dao.YakStore;
import com.flipkart.yaktest.failtest.utils.RandomUtil;
import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.flipkart.yaktest.interruption.models.SSHConfig;
import com.flipkart.yaktest.interruption.models.YakComponent;
import com.flipkart.yaktest.interruption.utils.YakUtils;
import com.flipkart.yaktest.models.BlockStatus;
import com.flipkart.yaktest.models.RegionInfo;
import com.flipkart.yaktest.output.*;
import com.flipkart.yaktest.utils.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.flipkart.yaktest.utils.CommonUtils.shutdownExecutor;

public class PostTestActivities {

    private static Logger logger = LoggerFactory.getLogger(PostTestActivities.class);

    public static void doWork(ProgramArguments programArguments, MetricRegistry registry) throws Exception {
        fetchAndUpdatePostTestStatus(programArguments, registry);
        updateTestStatus();
        updateComponentsProcessStatus();
        FileUtils.writeOutputToFile(TestOutput.INSTANCE);
    }

    private static void fetchAndUpdatePostTestStatus(ProgramArguments programArguments, MetricRegistry registry) throws Exception {
        OutputMetricResult outputMetricResult = TestOutput.INSTANCE.getTestStatus().getSepMetrices().get(SepOutputMetricName.CONNECTION_MISMATCH);

        String inconsistencies = YakUtils.checkInconsistencies(SSHConfig.getInstance()).replaceAll("[^0-9]+", "");
        int inconsistenciesCount = 1;
        if (!inconsistencies.equals("")) {
            inconsistenciesCount = Integer.parseInt(inconsistencies);
        }
        OutputMetricResult inconsistencyMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.INCONSISTENCIES);
        inconsistencyMetricResult.getFailureCount().addAndGet(inconsistenciesCount);
        inconsistencyMetricResult.setResult(inconsistenciesCount != 0);

        OutputMetricResult rackAwarenessValidatorMetricResult =
                TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.REGIONS_ARE_RACK_SPREAD);
        try {
            YakUtils.validateRegionRackAwareness(SSHConfig.getInstance());
            rackAwarenessValidatorMetricResult.setResult(true);
        } catch (ShellCommandException e) {
            rackAwarenessValidatorMetricResult.setResult(false);
        }

        BlockStatus blockStatus = WALIsolationChecker.getIsolationStatus();
        int failureCount = (int) (blockStatus.getTotalBlockCount() - blockStatus.getOkBlockCount());
        OutputMetricResult walIsolationMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.WAL_ISOLATED);
        walIsolationMetricResult.setResult(failureCount == 0);
        walIsolationMetricResult.getFailureCount().getAndAdd(failureCount);

        blockStatus = HBlockIsolationChecker.getIsolationStatus();
        failureCount = (int) (blockStatus.getTotalBlockCount() - blockStatus.getOkBlockCount());
        OutputMetricResult hBlockIsolationMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.HBLOCK_ISOLATED);
        hBlockIsolationMetricResult.setResult(failureCount == 0);
        hBlockIsolationMetricResult.getFailureCount().getAndAdd(failureCount);

        boolean isAllRegionsOpen = checkAllRegionsOpen();
        OutputMetricResult allRegionsOpenMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.ALL_REGIONS_OPEN);
        allRegionsOpenMetricResult.setResult(isAllRegionsOpen);

        int exceptionCount = tryPutGetDataInAllRegions(registry);
        OutputMetricResult failuresAfterTestMetricResult =
                TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.FAILURES_AFTER_TEST);
        failuresAfterTestMetricResult.setResult(exceptionCount > 0);
        failuresAfterTestMetricResult.getFailureCount().addAndGet(exceptionCount);
    }

    private static void updateTestStatus() {

        TestOutput.INSTANCE.getTestStatus().getSepMetrices().entrySet().forEach(entry -> {
            switch (entry.getKey()) {
                case ORDERING:
                    if (!entry.getValue().getResult()) {
                        entry.getValue().setStatus(Status.FAILED);
                    }
                    break;
                case REPETITION:
                case DATA_LOSS:
                case DATA_MISMATCH_RH:
                case DATA_MISMATCH_WH:
                case CONNECTION_MISMATCH:
                    if (entry.getValue().getResult()) {
                        entry.getValue().setStatus(Status.FAILED);
                    }
                    break;
            }
        });
        TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().entrySet().forEach(entry -> {
            switch (entry.getKey()) {
                case ALL_REGIONS_OPEN:
                case WAL_ISOLATED:
                case HBLOCK_ISOLATED:
                case REGIONS_ARE_RACK_SPREAD:
                    if (!entry.getValue().getResult()) {
                        entry.getValue().setStatus(Status.FAILED);
                    }
                    break;
                case GET_FAIL:
                case CHECK_PUT_EXCEPTION:
                case CHECK_PUT_FAIL:
                case INCONSISTENCIES:
                case DATA_LOSS:
                case DATA_MISMATCH_RH:
                case DATA_MISMATCH_WH:
                case FAILURES_AFTER_TEST:
                    if (entry.getValue().getResult()) {
                        entry.getValue().setStatus(Status.FAILED);
                    }
                    break;
            }
        });
        TestStatus testStatus = TestOutput.INSTANCE.getTestStatus();
        if (testStatus.getHbaseMetrices().get(HbaseOutputMetricName.DATA_LOSS).getResult() || testStatus.getSepMetrices()
                .get(SepOutputMetricName.DATA_LOSS).getResult() || testStatus.getHbaseMetrices().get(HbaseOutputMetricName.DATA_MISMATCH_WH).getResult()
                || testStatus.getSepMetrices().get(SepOutputMetricName.DATA_MISMATCH_WH).getResult() || testStatus.getHbaseMetrices()
                .get(HbaseOutputMetricName.DATA_MISMATCH_RH).getResult() || testStatus.getSepMetrices().get(SepOutputMetricName.DATA_MISMATCH_RH).getResult()
                || !testStatus.getSepMetrices().get(SepOutputMetricName.ORDERING).getResult()
                || !testStatus.getHbaseMetrices().get(HbaseOutputMetricName.REGIONS_ARE_RACK_SPREAD).getResult()) {
            TestOutput.INSTANCE.getTestStatus().setOverallStatus(Status.FAILED);
        }
    }

    private static void updateComponentsProcessStatus() {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<Pair<YakComponent, List<Future<ProcessStatus>>>> processesStatusFutures = Config.getInstance().getYakComponentConfig().entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue().stream()
                        .map(host -> executorService.submit(() -> YakUtils.isProcessRunning(host, entry.getKey(), SSHConfig.getInstance())))
                        .collect(Collectors.toList()))).collect(Collectors.toList());

        Map<YakComponent, List<ProcessStatus>> processesStatus =
                processesStatusFutures.stream().collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue().stream().map(processStatusFuture -> {
                    try {
                        return processStatusFuture.get();
                    } catch (InterruptedException e) {
                        logger.error("Interrupted {}", e.getMessage(), e);
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        logger.error("Could not complete Process Status Update {}", e.getMessage(), e);
                    }
                    return null;
                }).collect(Collectors.toList())));
        TestOutput.INSTANCE.setProcessesStatus(processesStatus);

        shutdownExecutor(executorService);
    }

    private static boolean checkAllRegionsOpen() throws ShellCommandException {
        Map<String, RegionInfo> regionsInfo = YakUtils.fetchRegionsInfo(SSHConfig.getInstance());
        List<String> openRegions = YakUtils.fetchOpenStateRegions(SSHConfig.getInstance());
        List<RegionInfo> openRegionsInfo = openRegions.stream().filter(regionsInfo::containsKey).map(regionsInfo::get)
                .sorted((o1, o2) -> o1.getRegionName().compareTo(o2.getRegionName())).collect(Collectors.toList());

        RegionInfo previousRegion = openRegionsInfo.get(0);
        int i;
        for (i = 1; i < openRegionsInfo.size(); i++) {
            RegionInfo nextRegion = openRegionsInfo.get(i);
            if (!previousRegion.getEndKey().equals(nextRegion.getStartKey())) {
                break;
            }
            previousRegion = nextRegion;
        }

        if (i == openRegionsInfo.size() && openRegionsInfo.get(0).getStartKey().equals("''") && openRegionsInfo.get(i - 1).getEndKey().equals("''")) {
            logger.info("All regions are in open state");
            return true;
        } else {
            logger.info("All regions are not in open state");
            return false;
        }
    }

    private static int tryPutGetDataInAllRegions(MetricRegistry registry) throws Exception {

        YakUtils.disableKafkaReplication(SSHConfig.getInstance());
        logger.info("Putting data in all regions for auto healing check");

        YakStore store =
                new YakStore(YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()),
                        YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup()),
                        Config.getInstance().getMultiZoneStoreConfig(), NoDistribution.INSTANCE, registry);

        ExecutorService executorService = Executors.newFixedThreadPool(20);

        final int dataSize = 1000;
        int regionGap = 10;
        int regionCount = 100 / regionGap;
        CountDownLatch latch = new CountDownLatch(regionCount);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        IntStream.range(0, regionCount).forEach(index -> {
            int startKey = index * regionGap;
            int endKey = (index + 1) * regionGap;
            executorService.submit(() -> {
                for (int i = 0; i < dataSize; i++) {
                    int distKey = startKey + RandomUtil.INSTANCE.randomInt(endKey - startKey);
                    String key = distKey + "-" + RandomUtil.INSTANCE.randomKey();
                    try {
                        store.put(key, 1, Optional.empty());
                        logger.debug("Put key {} ", key);
                    } catch (Exception e) {
                        logger.error("Put failure for Exception key {} ", key, e);
                        exceptionCount.incrementAndGet();
                    }

                    try {
                        store.verifyGet(key, 1, Optional.empty(), false);
                        logger.debug("Get key {} ", key);
                    } catch (Exception e) {
                        logger.error("Get failure for Exception key {} ", key, e);
                        exceptionCount.incrementAndGet();
                    }
                }
                latch.countDown();
            });
        });

        latch.await();

        shutdownExecutor(executorService);
        store.shutDown();

        return exceptionCount.get();
    }

    public static void main(String args[]) throws Exception {
        MetricRegistry registry = new MetricRegistry();
        JmxReporter reporter = JmxReporter.forRegistry(registry).build();
        reporter.start();
        tryPutGetDataInAllRegions(registry);
    }
}