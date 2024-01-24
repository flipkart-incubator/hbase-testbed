package com.flipkart.yaktest.failtest.models;

public enum TestCaseName {
    PUT_GET_TEST("putGetTest"),
    PUT_GET_KAFKA_TEST("putGetKafkaTest"),
    PUT_GET_PULSAR_TEST("putGetPulsarTest"),
    PUT_GET_BACKUP_TEST("putGetBackupTest"),
    PUT_GET_MMR_TEST("putGetMMRTest"),

    PUT_GET_HELIOS_COMPLETENESS_TEST("putGetHeliosCompletenessTest"),
    BLANK("blank");

    private String name;

    TestCaseName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
