package com.flipkart.yaktest.failtest.dao;

import java.util.Map;
import java.util.Optional;

public interface Store {

    void walRoll(Optional<String> routeKey) throws Exception;

    void put(String key, int version, Optional<String> routeKey) throws Exception;

    void batchPut(Map<String, Integer> keyVersionMap, Optional<String> routeKey) throws Exception;

    boolean checkPut(String key, int version, int expected, Optional<String> routeKey) throws Exception;

    void verifyGet(String key, Integer integer, Optional<String> routeKey, boolean queryFromBackup) throws Exception;

    void verifyGet(String key, Integer integer, Optional<String> routeKey, boolean queryFromBackup, int totalVersions) throws Exception;

    void shutDown() throws Exception;
}
