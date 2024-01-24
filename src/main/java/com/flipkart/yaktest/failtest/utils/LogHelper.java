package com.flipkart.yaktest.failtest.utils;

import com.flipkart.yak.distributor.KeyDistributor;

public enum LogHelper {
    INSTANCE;

    private KeyDistributor distributor;

    public void setup(KeyDistributor distributor) {
        this.distributor = distributor;
    }

    public String hbaseKey(String key) {
        return new String(distributor.enrichKey(key.getBytes()));
    }
}
