package com.flipkart.yaktest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yaktest.interruption.commons.RemoteService;
import com.flipkart.yaktest.interruption.models.YakComponent;
import com.flipkart.yaktest.models.BlockStatus;
import com.flipkart.yaktest.models.WalFileInfo;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.flipkart.yaktest.failtest.utils.BlockReportUtil.reloadBlockReportCache;
import static com.flipkart.yaktest.failtest.utils.NameNodeUtil.getActiveNamenode;

public class WALIsolationChecker {

    private WALIsolationChecker() {
    }

    private static Logger logger = LoggerFactory.getLogger(WALIsolationChecker.class);
    private static List<WalFileInfo> previousWalFileInfos = null;

    public static void updatePreviousWalFilesInfo() throws IOException, InterruptedException {
        previousWalFileInfos = fetchWALReport();
    }

    private static List<WalFileInfo> fetchWALReport() throws IOException, InterruptedException {

        String consoleEndpoint = Config.getInstance().getYakConsoleEndpoint();

        String namenode = getActiveNamenode();
        InetAddress address = InetAddress.getByName(namenode);
        logger.info("Found name-node IP: {}", address.getHostAddress());
        reloadBlockReportCache(namenode);
        Thread.sleep(5000);

        // https://github.com/flipkart-incubator/hbase-console
        String url = String.format("http://%s/fsview/%s/wals", consoleEndpoint, address.getHostAddress());

        ObjectMapper objectMapper = new ObjectMapper();
        List<WalFileInfo> walFileInfos = null;

        try {
            ResponseBody responseBody = RemoteService.get(url);
            logger.info("Fetched WalInfo, response {} ", responseBody);

            String walResponse = responseBody.string();
            logger.info("WalInfo Response {} ", walResponse);

            walFileInfos = objectMapper.readValue(walResponse, new TypeReference<List<WalFileInfo>>() {
            });
        } catch (Exception e) {
            logger.warn("no WAL isolation info found, will be reported as FAILED");
        }
        return walFileInfos;
    }

    public static BlockStatus getIsolationStatus() throws IOException, InterruptedException {

        List<WalFileInfo> walFileInfos = fetchWALReport();
        BlockStatus blockStatus = new BlockStatus();
        if (walFileInfos != null) {
            walFileInfos = filterWalFilesInfo(walFileInfos);
            List<String> servers = Config.getInstance().getYakComponentConfig().get(YakComponent.REGION_SERVER);

            long okWalFilesCount = walFileInfos.stream().filter(walFileInfo -> servers.contains(walFileInfo.getRegionServer().split(",")[0])).filter(
                            walFileInfo -> walFileInfo.getBlocks().stream()
                                    .filter(blockInfo -> !servers.contains(blockInfo.getPrimaryHost()) || !servers.containsAll(blockInfo.getSecondaryHosts())).count() == 0)
                    .count();
            blockStatus.setOkBlockCount(okWalFilesCount);
            blockStatus.setTotalBlockCount(walFileInfos.size());
            blockStatus.setDescription((walFileInfos.size() - okWalFilesCount) + " Wal files are in wrong group");
        }
        return blockStatus;
    }

    private static List<WalFileInfo> filterWalFilesInfo(List<WalFileInfo> walFileInfos) {

        Set<String> previousWalFiles = previousWalFileInfos.stream().map(WalFileInfo::getFileName).collect(Collectors.toSet());
        return walFileInfos.stream().filter(walFileInfo -> !previousWalFiles.contains(walFileInfo.getFileName())).collect(Collectors.toList());
    }
}
