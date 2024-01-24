package com.flipkart.yaktest.utils;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yaktest.BulkWriter;
import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.ProgramArguments;
import com.flipkart.yaktest.WALIsolationChecker;
import com.flipkart.yaktest.failtest.AbstractSepVerifierService;
import com.flipkart.yaktest.failtest.dao.YakStore;
import com.flipkart.yaktest.failtest.kafka.KafkaVerifierService;
import com.flipkart.yaktest.failtest.pulsar.PulsarVerifierService;
import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.flipkart.yaktest.interruption.models.SSHConfig;
import com.flipkart.yaktest.interruption.utils.YakUtils;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.flipkart.yaktest.utils.CommonUtils.shutdownExecutor;

public class PreTestActivities {

    private PreTestActivities() {
    }

    private static Logger logger = LoggerFactory.getLogger(PreTestActivities.class);
    private static final String DISABLE_REPLICATION_KEY = "-disableReplication";

    private static YakStore yakStore;

    private static YakStore getYakStoreInstance(MetricRegistry registry) throws Exception {
        if (yakStore == null) {
            yakStore = new YakStore(YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()),
                    YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup()),
                    Config.getInstance().getMultiZoneStoreConfig(), registry);
        }

        return yakStore;
    }

    public static void doWork(ProgramArguments programArguments, MetricRegistry registry) throws Exception {
        String sshUserName = programArguments.switchValue("-user", System.getProperty("user.name"));
        String jobId = String.valueOf(new Date().getTime());

        if (programArguments.switchPresent("-jobId")) {
            jobId = programArguments.switchValue("-jobId");
        }

        FileUtils.appendJobIdToLogsDir(jobId);
        FileUtils.createLogDirectory();

        if (programArguments.switchPresent("-configPath")) {
            Config.setCustomConfigPath(programArguments.switchValue("-configPath"));
        }

        YakStore store = getYakStoreInstance(registry);
        store.walRoll(Optional.empty());
        Thread.sleep(30000);

        startAllComponents(sshUserName);

        boolean disableReplication = (programArguments.switchPresent(DISABLE_REPLICATION_KEY) || programArguments.switchPresent("-preload"));
        refreshTable(Config.getInstance().getDefaultSite(), !disableReplication, Config.getInstance().getRsgroup());

        if (!programArguments.switchPresent(DISABLE_REPLICATION_KEY)) {
            drainSepTopic(programArguments.switchValue("-test"));
        }

        updateCurrentWalInfo();

        if (programArguments.switchPresent("-preload")) {
            populateData(registry);
        }

        if (!programArguments.switchPresent(DISABLE_REPLICATION_KEY)) {
            logger.info("enabling replication..");
            YakUtils.enableKafkaReplication(SSHConfig.getInstance());
        }

        if (programArguments.switchPresent(ProgramArguments.REPLICA_STORE)) {
            logger.info("enabling cluster replication..");
            YakUtils.enableClusterReplication(SSHConfig.getInstance());
            refreshTable(programArguments.switchValue(ProgramArguments.REPLICA_STORE), true, Config.getInstance().getReplicaRsgroup());
        }
    }

    private static void drainSepTopic(String testName) {

        logger.info("Cleaning up the {} replication topics. Waiting for 20 sec...", testName);
        AbstractSepVerifierService abstractSepVerifierService = null;

        if (testName.contains("Kafka")) {
            abstractSepVerifierService = new KafkaVerifierService();
        }
        if (testName.contains("Pulsar")) {
            abstractSepVerifierService = new PulsarVerifierService();
        }

        if (abstractSepVerifierService != null) {
            abstractSepVerifierService.consume();
            try {
                Thread.sleep(20000);
                abstractSepVerifierService.shutdown();
            } catch (InterruptedException e) {
                logger.error("could not drain before testing {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Cleanup is completed");
    }

    private static void updateCurrentWalInfo() throws Exception {
        WALIsolationChecker.updatePreviousWalFilesInfo();
    }

    private static void populateData(MetricRegistry registry) throws Exception {

        logger.info("Pre Loading Data..");

        YakStore store = getYakStoreInstance(registry);

        long startTime = System.currentTimeMillis();
        ExecutorService writeExec = Executors.newFixedThreadPool(20);

        List<Future<Integer>> futures = IntStream.range(0, 20).mapToObj(i -> writeExec.submit(new BulkWriter(137400, store))).collect(Collectors.toList());

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                logger.error("Could not populate data {}", e.getMessage(), e);
                future.cancel(true);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.error("Could not populate data {}", e.getMessage(), e);
            }
        });

        shutdownExecutor(writeExec);
        store.shutDown();

        logger.info("Data Loaded(2748000 rows) in {} ms", System.currentTimeMillis() - startTime);
    }

    private static void startAllComponents(String sshUserName) {
        if (Strings.isNullOrEmpty(SSHConfig.getInstance().getUser())) {
            SSHConfig.getInstance().setUser(sshUserName);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(20);

        Config.getInstance().getYakComponentConfig().entrySet().stream().map(entry -> entry.getValue().stream()
                .map(host -> executorService.submit(() -> YakUtils.startComponent(host, entry.getKey(), SSHConfig.getInstance())))
                .collect(Collectors.toList())).flatMap(List::stream).collect(Collectors.toList()).forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                logger.error("Could not execute {}", e.getMessage(), e);
            }
        });
        shutdownExecutor(executorService);
    }

    private static void refreshTable(String site, boolean enabledReplication, String rsgroup) throws ShellCommandException {
        YakUtils.createNamespace(SSHConfig.getInstance(), site, rsgroup);
        YakUtils.disableDropBackupTable(SSHConfig.getInstance(), site);
        YakUtils.dropAndCreateTable(SSHConfig.getInstance(), enabledReplication, site);
    }

}
