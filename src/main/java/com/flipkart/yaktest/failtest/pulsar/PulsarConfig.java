package com.flipkart.yaktest.failtest.pulsar;

import java.util.List;

public class PulsarConfig {
    private String endpoint;
    private String authEndpoint;
    private String authClientId;
    private String authClientSecret;
    private int numOfConsumer;
    private List<String> topics;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAuthEndpoint() {
        return authEndpoint;
    }

    public void setAuthEndpoint(String authEndpoint) {
        this.authEndpoint = authEndpoint;
    }

    public String getAuthClientId() {
        return authClientId;
    }

    public void setAuthClientId(String authClientId) {
        this.authClientId = authClientId;
    }

    public String getAuthClientSecret() {
        return authClientSecret;
    }

    public void setAuthClientSecret(String authClientSecret) {
        this.authClientSecret = authClientSecret;
    }

    public int getNumOfConsumer() {
        return numOfConsumer;
    }

    public void setNumOfConsumer(int numOfConsumer) {
        this.numOfConsumer = numOfConsumer;
    }

    @Override
    public String toString() {
        return "PulsarConfig{" +
                "endpoint='" + endpoint + '\'' +
                ", authEndpoint='" + authEndpoint + '\'' +
                ", authClientId='" + authClientId + '\'' +
                ", authClientSecret='" + authClientSecret + '\'' +
                ", numOfConsumer=" + numOfConsumer +
                ", topics=" + topics +
                '}';
    }
}
