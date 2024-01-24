package com.flipkart.yaktest.interruption.models;

public enum ShellExitStatus {

    SUCCESS(0),
    FAIL(-1);

    int status;

    ShellExitStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "{ name " + name() + " code : " + status + " }";
    }
}
