package com.flipkart.yaktest.output;

import java.util.ArrayList;
import java.util.List;

public class InterruptionStatus {

    public enum Status {
        SUCCESS,
        FAIL;
    }

    private Status status = Status.SUCCESS;
    private int count;
    private List<TimeDuration> interruptionTimes = new ArrayList<>();

    public InterruptionStatus() {

    }

    public InterruptionStatus(int count) {
        this.count = count;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<TimeDuration> getInterruptionTimes() {
        return interruptionTimes;
    }

    public InterruptionStatus buildInterruptionStatus(int count, Status status) {
        InterruptionStatus interruptionStatus = new InterruptionStatus(count);
        interruptionStatus.status = status;
        return interruptionStatus;
    }

    @Override
    public String toString() {
        return "InterruptionStatus{" + "status=" + status + ", count=" + count + '}';
    }
}
