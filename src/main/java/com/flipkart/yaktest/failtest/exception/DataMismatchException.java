package com.flipkart.yaktest.failtest.exception;

public class DataMismatchException extends Exception {

    public enum Type {
        READ_VERSION_HIGHER,
        WRITE_VERSION_HIGHER,
        PAYLOAD_MISMATCH,
        TOTAL_VERSIONS_MISMATCH;
    }

    private final Type type;

    public DataMismatchException(Type type, String message) {
        super("Mismatch Type " + type.name() + " " + message);
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}
