package com.flipkart.yaktest.failtest.utils;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.interruption.commons.RemoteService;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BlockReportUtil {

    private BlockReportUtil() {
    }

    private static Logger logger = LoggerFactory.getLogger(BlockReportUtil.class);

    public static void reloadBlockReportCache(String namenode) throws IOException {
        String consoleEndpoint = Config.getInstance().getYakConsoleEndpoint();
        // https://github.com/flipkart-incubator/hbase-console
        String url = String.format("http://%s/fsview/%s/reload", consoleEndpoint, namenode);
        ResponseBody responseBody = RemoteService.get(url);
        logger.debug("Received response {}", responseBody);
    }
}
