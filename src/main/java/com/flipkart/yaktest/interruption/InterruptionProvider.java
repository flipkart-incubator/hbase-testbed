package com.flipkart.yaktest.interruption;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.failtest.utils.HMasterUtil;
import com.flipkart.yaktest.failtest.utils.NameNodeUtil;
import com.flipkart.yaktest.failtest.utils.RandomUtil;
import com.flipkart.yaktest.interruption.annotation.Interruption;
import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.flipkart.yaktest.interruption.models.InterruptionName;
import com.flipkart.yaktest.interruption.models.SSHConfig;
import com.flipkart.yaktest.interruption.models.YakComponent;
import com.flipkart.yaktest.interruption.utils.YakUtils;
import com.flipkart.yaktest.output.InterruptionStatus;
import com.flipkart.yaktest.output.TestOutput;
import com.flipkart.yaktest.output.TimeDuration;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.yaktest.interruption.utils.YakUtils.*;

public class InterruptionProvider {

    private Logger logger = LoggerFactory.getLogger(InterruptionProvider.class);

    private int interruptionDuration;
    private int interruptionGap;
    private SSHConfig sshConfig;
    private static final String KAKFA_VALUE_KEY = "value";

    public InterruptionProvider(int interruptionDuration, int interruptionGap) {
        this.sshConfig = SSHConfig.getInstance();
        this.interruptionDuration = interruptionDuration;
        this.interruptionGap = interruptionGap;
    }

    @Interruption(name = InterruptionName.KILL_REGION_SERVER)
    public void killRegionServer(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        killAndStartComponents(YakComponent.REGION_SERVER, noOfHost, populateInterruptionStatus(InterruptionName.KILL_REGION_SERVER, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.STOP_REGION_SERVER)
    public void stopRegionServer(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        stopAndStartComponents(YakComponent.REGION_SERVER, noOfHost, populateInterruptionStatus(InterruptionName.STOP_REGION_SERVER, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.KILL_DATA_NODE)
    public void killDataNode(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        killAndStartComponents(YakComponent.DATA_NODE, noOfHost, populateInterruptionStatus(InterruptionName.KILL_DATA_NODE, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.STOP_DATA_NODE)
    public void stopDataNode(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        stopAndStartComponents(YakComponent.DATA_NODE, noOfHost, populateInterruptionStatus(InterruptionName.STOP_DATA_NODE, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.KILL_ZOOKEEPER)
    public void killZookeeper(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        killAndStartComponents(YakComponent.ZOOKEEPER, noOfHost, populateInterruptionStatus(InterruptionName.KILL_ZOOKEEPER, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.STOP_ZOOKEEPER)
    public void stopZookeeper(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        stopAndStartComponents(YakComponent.ZOOKEEPER, noOfHost, populateInterruptionStatus(InterruptionName.STOP_ZOOKEEPER, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.KILL_JOURNAL_NODE)
    public void killJournalNode(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        killAndStartComponents(YakComponent.JOURNAL_NODE, noOfHost, populateInterruptionStatus(InterruptionName.KILL_JOURNAL_NODE, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.STOP_JOURNAL_NODE)
    public void stopJN(int noOfHost, boolean isParallel) throws
            ShellCommandException, InterruptedException {
        stopAndStartComponents(YakComponent.JOURNAL_NODE, noOfHost, populateInterruptionStatus(InterruptionName.STOP_JOURNAL_NODE, noOfHost), isParallel);
    }

    @Interruption(name = InterruptionName.KILL_H_MASTER)
    public void killHMaster(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> hosts = Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER);
        for (String host : hosts) {
            killAndStartComponents(YakComponent.MASTER, Collections.singletonList(host),
                    populateInterruptionStatus(InterruptionName.KILL_H_MASTER, noOfHost));
        }
    }

    @Interruption(name = InterruptionName.STOP_H_MASTER)
    public void stopHMaster(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> hosts = Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER);
        for (String host : hosts) {
            stopAndStartComponents(YakComponent.MASTER, Collections.singletonList(host),
                    populateInterruptionStatus(InterruptionName.STOP_H_MASTER, noOfHost));
        }
    }

    @Interruption(name = InterruptionName.NETWORK_PARTITION_DATA_NODE)
    public void networkPartitionDataNode(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        createAndRemoveNetworkPartition(YakComponent.DATA_NODE, noOfHost, populateInterruptionStatus(InterruptionName.NETWORK_PARTITION_DATA_NODE, noOfHost),
                isParallel);
    }

    @Interruption(name = InterruptionName.NETWORK_PARTITION_MASTER)
    public void networkPartitionMaster(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException, IOException {
        createAndRemoveNetworkPartition(Collections.singletonList(HMasterUtil.getActiveMaster()),
                populateInterruptionStatus(InterruptionName.NETWORK_PARTITION_MASTER, noOfHost));
    }

    @Interruption(name = InterruptionName.NETWORK_PARTITION_NAME_NODE)
    public void networkPartitionNameNode(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException, IOException {
        createAndRemoveNetworkPartition(Collections.singletonList(NameNodeUtil.getActiveNamenode()),
                populateInterruptionStatus(InterruptionName.NETWORK_PARTITION_NAME_NODE, noOfHost));
    }

    @Interruption(name = InterruptionName.NETWORK_PARTITION_ZK)
    public void networkPartitionZK(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        createAndRemoveNetworkPartition(YakComponent.ZOOKEEPER, noOfHost, populateInterruptionStatus(InterruptionName.NETWORK_PARTITION_ZK, noOfHost),
                isParallel);
    }

    @Interruption(name = InterruptionName.NETWORK_PARTITION_JN)
    public void networkPartitionJN(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        createAndRemoveNetworkPartition(YakComponent.JOURNAL_NODE, noOfHost, populateInterruptionStatus(InterruptionName.NETWORK_PARTITION_JN, noOfHost),
                isParallel);
    }

    @Interruption(name = InterruptionName.SPLIT_REGIONS)
    public void splitRegions(int noOfRegions, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        splitRegions(noOfRegions, populateInterruptionStatus(InterruptionName.SPLIT_REGIONS, noOfRegions), isParallel);
    }

    @Interruption(name = InterruptionName.MERGE_REGIONS)
    public void mergeRegions(int noOfMerge, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        mergeRegions(noOfMerge, populateInterruptionStatus(InterruptionName.MERGE_REGIONS, noOfMerge), isParallel);
    }

    @Interruption(name = InterruptionName.MASTER_NAMENODE_ACROSS_NETWORK)
    public void makeMasterNamenodeAcrossNetwork(int noOfHost, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> masters = Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER);
        List<String> namenodes = Config.getInstance().getYakComponentConfig().get(YakComponent.NAME_NODE);

        String activeMaster = masters.get(0);
        List<String> masterToStop = masters.stream().filter(master -> (!master.equals(activeMaster))).collect(Collectors.toList());
        String activeNameNode = masterToStop.get(0);
        List<String> namenodesToStop = namenodes.stream().filter(namenode -> (!namenode.equals(activeNameNode))).collect(Collectors.toList());

        stopAndStartComponents(YakComponent.MASTER, masterToStop, populateInterruptionStatus(InterruptionName.MASTER_NAMENODE_ACROSS_NETWORK, noOfHost));
        stopAndStartComponents(YakComponent.NAME_NODE, namenodesToStop, populateInterruptionStatus(InterruptionName.MASTER_NAMENODE_ACROSS_NETWORK, noOfHost));
    }

    private List<String> getRandomHosts(YakComponent component, int noOfHost) {

        List<String> hosts = Config.getInstance().getYakComponentConfig().get(component);
        List<Integer> indexes = RandomUtil.INSTANCE.randomInts(hosts.size(), noOfHost);
        return indexes.stream().map(hosts::get).collect(Collectors.toList());
    }

    private void splitRegions(int noOfRegions, InterruptionStatus interruptionStatus, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> regions = YakUtils.getRegions(sshConfig);

        List<Integer> indexes = RandomUtil.INSTANCE.randomInts(regions.size(), noOfRegions);
        List<String> regionsToSplit = indexes.stream().map(regions::get).collect(Collectors.toList());

        if (isParallel) {
            splitRegions(regionsToSplit, interruptionStatus);
        } else {
            for (String region : regionsToSplit) {
                splitRegions(Collections.singletonList(region), interruptionStatus);
            }
        }
    }

    private void mergeRegions(int noOfMerge, InterruptionStatus interruptionStatus, boolean isParallel)
            throws InterruptedException, ShellCommandException {
        List<String> regions = YakUtils.fetchEncodedRegions(sshConfig);

        int j = regions.size() - (2 * noOfMerge);
        List<Pair<String, String>> regionsPairToMerge = new ArrayList<>();
        while (j + 1 < regions.size()) {
            if (j >= 0) {
                regionsPairToMerge.add(Pair.of(regions.get(j), regions.get(j + 1)));
            }
            j = j + 2;
        }

        if (isParallel) {
            mergeRegions(regionsPairToMerge, interruptionStatus);
        } else {
            for (Pair<String, String> regionPair : regionsPairToMerge) {
                mergeRegions(Collections.singletonList(regionPair), interruptionStatus);
            }
        }
    }

    private void splitRegions(List<String> regions, InterruptionStatus interruptionStatus) throws InterruptedException {
        Thread.sleep(interruptionGap);
        logger.info("started region split");
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        for (String region : regions) {
            try {
                YakUtils.splitRegion(region, sshConfig);
                successCount++;
            } catch (ShellCommandException e) {
                //pass
            }
        }
        if (successCount == 0) {
            interruptionStatus.setStatus(InterruptionStatus.Status.FAIL);
        }
        updateInterruptionDurationWithCurrentEndTime(startTime, interruptionStatus);
    }

    private void mergeRegions(List<Pair<String, String>> regionsPair, InterruptionStatus interruptionStatus) throws InterruptedException {
        Thread.sleep(interruptionGap);
        logger.info("started region merge");
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        for (Pair<String, String> regionPair : regionsPair) {
            try {
                YakUtils.mergeRegions(regionPair.getLeft(), regionPair.getRight(), sshConfig);
                successCount++;
            } catch (ShellCommandException e) {
                //pass
            }
        }
        if (successCount == 0) {
            interruptionStatus.setStatus(InterruptionStatus.Status.FAIL);
        }
        updateInterruptionDurationWithCurrentEndTime(startTime, interruptionStatus);
    }

    private void killAndStartComponents(YakComponent yakComponent, List<String> hosts, InterruptionStatus interruptionStatus)
            throws ShellCommandException, InterruptedException {
        Thread.sleep(interruptionGap);
        long startTime = System.currentTimeMillis();
        try {
            killComponents(hosts, yakComponent, sshConfig);
        } catch (ShellCommandException e) {
            interruptionStatus.setStatus(InterruptionStatus.Status.FAIL);
            throw e;
        }

        Thread.sleep(interruptionDuration);
        startComponents(hosts, yakComponent, sshConfig);
        updateInterruptionDurationWithCurrentEndTime(startTime, interruptionStatus);
    }

    private void stopAndStartComponents(YakComponent yakComponent, List<String> hosts, InterruptionStatus interruptionStatus)
            throws ShellCommandException, InterruptedException {
        Thread.sleep(interruptionGap);
        long startTime = System.currentTimeMillis();
        try {
            stopComponents(hosts, yakComponent, sshConfig);
        } catch (ShellCommandException e) {
            interruptionStatus.setStatus(InterruptionStatus.Status.FAIL);
            throw e;
        }

        Thread.sleep(interruptionDuration);
        startComponents(hosts, yakComponent, sshConfig);
        updateInterruptionDurationWithCurrentEndTime(startTime, interruptionStatus);
    }

    private void createAndRemoveNetworkPartition(List<String> hosts, InterruptionStatus interruptionStatus)
            throws ShellCommandException, InterruptedException {

        Thread.sleep(interruptionGap);
        long startTime = System.currentTimeMillis();
        try {
            createPartition(hosts, sshConfig);
        } catch (ShellCommandException e) {
            interruptionStatus.setStatus(InterruptionStatus.Status.FAIL);
            throw e;
        }

        Thread.sleep(interruptionDuration);
        removePartition(hosts, sshConfig);
        updateInterruptionDurationWithCurrentEndTime(startTime, interruptionStatus);
    }

    private void killAndStartComponents(YakComponent yakComponent, int noOfHost, InterruptionStatus interruptionStatus, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> hosts = getRandomHosts(yakComponent, noOfHost);
        if (isParallel) {
            killAndStartComponents(yakComponent, hosts, interruptionStatus);
        } else {
            for (String host : hosts) {
                killAndStartComponents(yakComponent, Collections.singletonList(host), interruptionStatus);
            }
        }
    }

    private void stopAndStartComponents(YakComponent yakComponent, int noOfHost, InterruptionStatus interruptionStatus, boolean isParallel)
            throws ShellCommandException, InterruptedException {
        List<String> hosts = getRandomHosts(yakComponent, noOfHost);
        if (isParallel) {
            stopAndStartComponents(yakComponent, hosts, interruptionStatus);
        } else {
            for (String host : hosts) {
                stopAndStartComponents(yakComponent, Collections.singletonList(host), interruptionStatus);
            }
        }
    }

    private void createAndRemoveNetworkPartition(YakComponent yakComponent, int noOfHost, InterruptionStatus interruptionStatus, boolean isParallel)
            throws ShellCommandException, InterruptedException {

        List<String> hosts = getRandomHosts(yakComponent, noOfHost);
        if (isParallel) {
            createAndRemoveNetworkPartition(hosts, interruptionStatus);
        } else {
            for (String host : hosts) {
                createAndRemoveNetworkPartition(Collections.singletonList(host), interruptionStatus);
            }
        }
    }

    private InterruptionStatus populateInterruptionStatus(InterruptionName interruptionName, int count) {
        InterruptionStatus interruptionStatus = TestOutput.INSTANCE.getInterruptionsStatus().get(interruptionName);
        if (interruptionStatus == null) {
            interruptionStatus = new InterruptionStatus(count);
            TestOutput.INSTANCE.getInterruptionsStatus().put(interruptionName, interruptionStatus);
        }
        return interruptionStatus;
    }

    private void updateInterruptionDurationWithCurrentEndTime(long startTime, InterruptionStatus interruptionStatus) {
        long endTime = System.currentTimeMillis();
        interruptionStatus.getInterruptionTimes().add(new TimeDuration(new Date(startTime), new Date(endTime), endTime - startTime));
    }
}
