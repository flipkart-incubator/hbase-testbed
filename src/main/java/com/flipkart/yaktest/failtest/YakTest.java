package com.flipkart.yaktest.failtest;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.failtest.annotation.Test;
import com.flipkart.yaktest.failtest.dao.YakStore;
import com.flipkart.yaktest.failtest.kafka.KafkaVerifierService;
import com.flipkart.yaktest.failtest.utils.LogHelper;
import com.flipkart.yaktest.failtest.pulsar.PulsarVerifierService;
import com.flipkart.yaktest.interruption.models.SSHConfig;
import com.flipkart.yaktest.interruption.utils.YakUtils;
import com.flipkart.yaktest.output.TestOutput;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.flipkart.yaktest.failtest.models.TestCaseName.*;

public class YakTest {

    private static Logger logger = LoggerFactory.getLogger(YakTest.class);
    private int repeats;
    private int dataSize = 97;
    private int runSize = 109;
    private int totalVersions = 5;
    private ExecutorService writeExec;
    private ExecutorService readExec;
    private YakStore store;
    private AtomicInteger writeMessageCount = new AtomicInteger(0);
    private Optional<String> routeKey;

    public YakTest(int concc, int repeats, MetricRegistry registry) throws Exception {
        this.repeats = repeats;
        this.writeExec = Executors.newFixedThreadPool(concc, new ThreadFactoryBuilder().setNameFormat("write-thread-%d").build());
        this.readExec = Executors.newFixedThreadPool(concc, new ThreadFactoryBuilder().setNameFormat("read-thread-%d").build());
        this.store =
                new YakStore(YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()),
                        YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup()),
                        Config.getInstance().getMultiZoneStoreConfig(), registry);
        LogHelper.INSTANCE.setup(store.getKeyDistributor());
        routeKey = Optional.empty();
    }

    public void setRouteKey(String routeKey) {
        this.routeKey = Optional.of(routeKey);
    }

    @Test(name = PUT_GET_TEST)
    public void putGetTest() throws Exception {
        long startTime = System.currentTimeMillis();

        TestOutput.INSTANCE.setTestCaseName(PUT_GET_TEST);
        // Write data
        TestOutput.INSTANCE.getRunningTime().getPutTime().setStartTime(new Date(startTime));
        List<Future<Map<String, Integer>>> futures = submitWrites(writeMessageCount);

        if (routeKey.isPresent()) {
            verifyReadsWithRoute(futures, startTime, routeKey.get(), false, false);
            return;
        }
        //Read and Check Data sanity
        logger.info("Starting to verify reads");
        verifyReads(futures, startTime, false, false);
    }

    @Test(name = PUT_GET_HELIOS_COMPLETENESS_TEST)
    public void putGetHeliosCompletenessTest() throws Exception {
        long startTime = System.currentTimeMillis();

        TestOutput.INSTANCE.setTestCaseName(PUT_GET_HELIOS_COMPLETENESS_TEST);
        // Write data
        TestOutput.INSTANCE.getRunningTime().getPutTime().setStartTime(new Date(startTime));
        List<Future<Map<String, Integer>>> futures = submitWritesWithMultiVersions(writeMessageCount);

        if (routeKey.isPresent()) {
            logger.info("Starting to verify Helios  reads");
            verifyReadsWithRoute(futures, startTime, routeKey.get(), true, false);
        } else {
            logger.error("Exiting as no Route Key Present to Verify helios Reads");
        }
    }

    @Test(name = PUT_GET_MMR_TEST)
    public void putGetMMRTest() throws Exception {
        long startTime = System.currentTimeMillis();

        TestOutput.INSTANCE.setTestCaseName(PUT_GET_MMR_TEST);
        // Write data
        TestOutput.INSTANCE.getRunningTime().getPutTime().setStartTime(new Date(startTime));
        List<Future<Map<String, Integer>>> futures = submitMMRWrites(writeMessageCount, routeKey.get());

        if (routeKey.isPresent()) {
            logger.info("Starting to verify MMR reads");
            verifyReadsWithRoute(futures, startTime, routeKey.get(), false, true);
        } else {
            logger.error("Exiting as no Route Key Present to Verify MMR Reads");
        }
    }

    private void sepPutGetTest(AbstractSepVerifierService abstractSepVerifierService) throws Exception {
        long startTime = System.currentTimeMillis();

        abstractSepVerifierService.consume();
        TestOutput.INSTANCE.getRunningTime().getKafkaTime().setStartTime(new Date(startTime));

        // Write data
        TestOutput.INSTANCE.getRunningTime().getPutTime().setStartTime(new Date(startTime));
        List<Future<Map<String, Integer>>> futures = submitWrites(writeMessageCount);

        //Read and Check Data sanity
        ConcurrentMap<String, Integer> writeData = verifyReads(futures, startTime, false, false);

        abstractSepVerifierService.awaitReplicationLag();
        abstractSepVerifierService.verifyData(writeData);
        long endTime = System.currentTimeMillis();
        TestOutput.INSTANCE.getRunningTime().getKafkaTime().setEndTime(new Date(endTime));
        TestOutput.INSTANCE.getRunningTime().getKafkaTime().setDuration(endTime - startTime);
        abstractSepVerifierService.shutdown();
    }

    @Test(name = PUT_GET_KAFKA_TEST)
    public void putGetKafkaTest() throws Exception {
        AbstractSepVerifierService kafkaVerifierService = new KafkaVerifierService();
        TestOutput.INSTANCE.setTestCaseName(PUT_GET_KAFKA_TEST);
        this.sepPutGetTest(kafkaVerifierService);
    }

    @Test(name = PUT_GET_PULSAR_TEST)
    public void putGetPulsarTest() throws Exception {
        AbstractSepVerifierService pulsarVerifierService = new PulsarVerifierService();
        TestOutput.INSTANCE.setTestCaseName(PUT_GET_PULSAR_TEST);
        this.sepPutGetTest(pulsarVerifierService);
    }

    @Test(name = PUT_GET_BACKUP_TEST)
    public void putGetBackupTest() throws Exception {
        long startTime = System.currentTimeMillis();

        TestOutput.INSTANCE.setTestCaseName(PUT_GET_BACKUP_TEST);
        // Write data
        TestOutput.INSTANCE.getRunningTime().getPutTime().setStartTime(new Date(startTime));
        List<Future<Map<String, Integer>>> futures = submitWrites(writeMessageCount);

        //Wait for all writes to complte
        for (int i = 0; i < repeats; i++) {
            futures.get(i).get();
        }

        // Backup Data
        YakUtils.createBackupSet(SSHConfig.getInstance());
        YakUtils.performBackup(SSHConfig.getInstance());

        // Restore Data
        YakUtils.performRestore(SSHConfig.getInstance());

        //Read and Check Data sanity with new table
        logger.info("Starting to verify reads");
        verifyReads(futures, startTime, true, false);
    }

    private ConcurrentMap<String, Integer> verifyReads(List<Future<Map<String, Integer>>> futures, long startTime,
                                                       boolean queryForBackup, boolean multiVersionFlag) throws Exception {
        final ConcurrentMap<String, Integer> writeData = new ConcurrentHashMap<>(dataSize * repeats);
        final CountDownLatch latch = new CountDownLatch(repeats);

        TestOutput.INSTANCE.getRunningTime().getGetTime().setStartTime(new Date(startTime));

        for (int i = 0; i < repeats; i++) {
            final Map<String, Integer> data = futures.get(i).get();
            writeData.putAll(data);
            if (!multiVersionFlag) {
                readExec.execute(new GetVerifyRunner(latch, data, store, null, queryForBackup));
            } else {
                readExec.execute(new GetVerifyRunner(latch, data, store, null, queryForBackup, true, 5));
            }
        }
        terminateReadVerification(startTime, latch);
        return writeData;
    }

    private ConcurrentMap<String, Integer> verifyReadsWithRoute(List<Future<Map<String, Integer>>> futures,
                                                                long startTime, String route, boolean multiVersionFlag, boolean dualDCPutFlag) throws Exception {
        final ConcurrentMap<String, Integer> writeData = new ConcurrentHashMap<>(dataSize * repeats);
        ConcurrentHashMap<Integer, Map<String, Integer>> data = new ConcurrentHashMap<>();
        TestOutput.INSTANCE.getRunningTime().getGetTime().setStartTime(new Date(startTime));
        for (int i = 0; i < repeats; i++) {
            final Map<String, Integer> temp = futures.get(i).get();
            writeData.putAll(temp);
            data.put(i, temp);
        }
        store.walRoll(Optional.empty());
        logger.info("Sleeping for {} to fill up replication buffer", Config.getInstance().getReplicationBuffer());
        Thread.sleep(Config.getInstance().getReplicationBuffer());
        final CountDownLatch latch = new CountDownLatch(repeats);
        for (int i = 0; i < repeats; i++) {
            if (multiVersionFlag) {
                readExec.execute(new GetVerifyRunner(latch, data.get(i), store, route, false, true, 5));
            } else {
                if (dualDCPutFlag) {
                    readExec.execute(new GetVerifyRunner(latch, data.get(i), store, route, false, true));
                } else {
                    readExec.execute(new GetVerifyRunner(latch, data.get(i), store, route, false));
                }
            }
        }
        terminateReadVerification(startTime, latch);
        return writeData;
    }

    private void terminateReadVerification(long startTime, CountDownLatch latch) throws Exception {
        long putEndTime = System.currentTimeMillis();
        TestOutput.INSTANCE.getRunningTime().getPutTime().setEndTime(new Date(putEndTime));
        TestOutput.INSTANCE.getRunningTime().getPutTime().setDuration(putEndTime - startTime);
        logger.info("All writes done in {} ms .. waiting on reads ", (putEndTime - startTime));
        stopExecutor(writeExec);

        latch.await();

        long getEndTime = System.currentTimeMillis();
        TestOutput.INSTANCE.getRunningTime().getGetTime().setEndTime(new Date(getEndTime));
        TestOutput.INSTANCE.getRunningTime().getGetTime().setDuration(getEndTime - startTime);
        logger.info("Total write count {} ", writeMessageCount);
        logger.info("All reads done in {} ms ", (getEndTime - startTime));
        stopExecutor(readExec);
        store.shutDown();
    }

    private List<Future<Map<String, Integer>>> submitWritesWithMultiVersions(AtomicInteger writeMessageCount) throws Exception {
        // Write data
        List<Future<Map<String, Integer>>> futures = new ArrayList<>(repeats);
        logger.info("Submitting writes for multiple versions with repeats ", repeats);
        for (int i = 0; i < repeats; i++) {
            futures.add(writeExec.submit(new WriteRunner(dataSize, runSize, store, writeMessageCount, i, null, true, 5)));
        }
        logger.info("All writes submitted with multiple versions");
        return futures;
    }

    private List<Future<Map<String, Integer>>> submitMMRWrites(AtomicInteger writeMessageCount, String route) throws Exception {
        // Write data
        List<Future<Map<String, Integer>>> futures = new ArrayList<>(repeats);
        logger.info("Submitting writes for multiple versions with repeats ", repeats);
        for (int i = 0; i < repeats; i++) {
            futures.add(writeExec.submit(new WriteRunner(dataSize, runSize, store, writeMessageCount, i, route, true)));
        }
        logger.info("All writes submitted with multiple versions");
        return futures;
    }

    private List<Future<Map<String, Integer>>> submitWrites(AtomicInteger writeMessageCount) throws Exception {
        // Write data
        List<Future<Map<String, Integer>>> futures = new ArrayList<>(repeats);
        for (int i = 0; i < repeats; i++) {
            futures.add(writeExec.submit(new WriteRunner(dataSize, runSize, store, writeMessageCount, i, null)));
        }
        logger.info("All writes submitted");
        return futures;
    }

    private static void stopExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }
}
