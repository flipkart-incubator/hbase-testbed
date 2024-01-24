package com.flipkart.yaktest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.yaktest.interruption.commons.RemoteService;
import com.flipkart.yaktest.interruption.models.YakComponent;
import com.flipkart.yaktest.interruption.utils.YakUtils;
import com.flipkart.yaktest.models.BlockStatus;
import com.flipkart.yaktest.models.HBlockMetricsByNode;
import com.flipkart.yaktest.models.RegionBlockMetrics;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.flipkart.yaktest.failtest.utils.BlockReportUtil.reloadBlockReportCache;
import static com.flipkart.yaktest.failtest.utils.NameNodeUtil.getActiveNamenode;

public class HBlockIsolationChecker {

    private static Logger logger = LoggerFactory.getLogger(HBlockIsolationChecker.class);

    private static HBlockMetricsByNode fetchHBlockMetrices() throws Exception {
        String consoleEndpoint = Config.getInstance().getYakConsoleEndpoint();

        String namenode = getActiveNamenode();
        InetAddress address = InetAddress.getByName(namenode);
        logger.info("Found name-node IP: {}", address.getHostAddress());
        reloadBlockReportCache(namenode);
        Thread.sleep(5000);

        // https://github.com/flipkart-incubator/hbase-console
        String url = String.format("http://%s/fsview/%s/tableByNode", consoleEndpoint, address.getHostAddress());
        Map<String, HBlockMetricsByNode> hBlockMetricsByNodeTableMap = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            ResponseBody responseBody = RemoteService.get(url);

            logger.info("Fetched HBlockInfo, response {} ", responseBody);

            String hBlockResponse = responseBody.string();
            logger.info("HBlockInfo Response {} ", hBlockResponse);
            hBlockMetricsByNodeTableMap = objectMapper.readValue(hBlockResponse, new TypeReference<Map<String, HBlockMetricsByNode>>() {
            });
        } catch (Exception e) {
            logger.warn("No WAL isolation data found, sending empty metrics", e);
            return null;
        }
        return hBlockMetricsByNodeTableMap.get(YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()));
    }

    public static BlockStatus getIsolationStatus() throws Exception {
        BlockStatus blockStatus = new BlockStatus();
        HBlockMetricsByNode hBlockMetricsByNode = fetchHBlockMetrices();
        if (hBlockMetricsByNode == null) {
            return blockStatus;
        }
        Map<String, RegionBlockMetrics> nodeWiseMetrics = hBlockMetricsByNode.getNodes();

        List<String> dataNodes = Config.getInstance().getYakComponentConfig().get(YakComponent.DATA_NODE);

        AtomicLong okCount = new AtomicLong(0);
        AtomicLong totalCount = new AtomicLong(0);
        nodeWiseMetrics.entrySet().stream().forEach(entry -> {
            long blockCount = entry.getValue().getPrimaryBlocks() + entry.getValue().getReplicaBlocks();
            if (dataNodes.contains(entry.getKey())) {
                okCount.getAndAdd(blockCount);
            } else {
                logger.debug(String.format("HblockIsolation Failed. Blocks: %s is on node %s", blockCount, entry.getKey()));
            }
            totalCount.getAndAdd(blockCount);
        });

        fetchHBlockMetrices();
        blockStatus.setOkBlockCount(okCount.get());
        blockStatus.setTotalBlockCount(totalCount.get());
        return blockStatus;
    }
}
