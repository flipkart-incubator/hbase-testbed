package com.flipkart.yaktest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CommonUtils {

    private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    public static void shutdownExecutor(ExecutorService executorService) {
        executorService.shutdown();
        boolean done = false;
        try {
            done = executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug(e.getMessage(), e);
        }
        if (!done) {
            executorService.shutdownNow();
        }
    }
}
