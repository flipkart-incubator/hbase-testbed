package com.flipkart.yaktest.output;

public class ProcessStatus {

    private String host;
    private boolean isRunning = true;

    public ProcessStatus() {

    }

    public ProcessStatus(String host, boolean isRunning) {
        this.host = host;
        this.isRunning = isRunning;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "ProcessStatus{" + "host='" + host + '\'' + ", isRunning=" + isRunning + '}';
    }
}
