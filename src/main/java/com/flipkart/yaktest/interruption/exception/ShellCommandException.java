package com.flipkart.yaktest.interruption.exception;

import com.flipkart.yaktest.interruption.models.ShellExitStatus;

public class ShellCommandException extends Exception {

    private ShellExitStatus status;

    public ShellCommandException(ShellExitStatus status) {
        super("command status : " + status);
        this.status = status;
    }

    public ShellCommandException(ShellExitStatus status, String message) {
        super("command status : " + status + "\n" + message);
        this.status = status;
    }

    public ShellCommandException(ShellExitStatus status, Throwable e) {
        super("command status : " + status, e);
        this.status = status;
    }

    public ShellCommandException(ShellExitStatus status, String message, Throwable e) {
        super("command status : " + status + "\n" + message, e);
        this.status = status;
    }

    public ShellExitStatus getStatus() {
        return status;
    }
}
