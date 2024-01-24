package com.flipkart.yaktest.output;

import java.util.Date;

public class TimeDuration {

    private Date startTime;
    private Date endTime;
    private long duration;

    TimeDuration() {

    }

    public TimeDuration(Date startTime, Date endTime, long duration) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "TimeDuration{" + "startTime=" + startTime + ", endTime=" + endTime + ", duration=" + duration + '}';
    }
}
