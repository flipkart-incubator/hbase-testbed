package com.flipkart.yaktest.output;

public class RunningTime {

    private TimeDuration getTime = new TimeDuration();
    private TimeDuration putTime = new TimeDuration();
    private TimeDuration kafkaTime = new TimeDuration();

    public TimeDuration getGetTime() {
        return getTime;
    }

    public void setGetTime(TimeDuration getTime) {
        this.getTime = getTime;
    }

    public TimeDuration getPutTime() {
        return putTime;
    }

    public void setPutTime(TimeDuration putTime) {
        this.putTime = putTime;
    }

    public TimeDuration getKafkaTime() {
        return kafkaTime;
    }

    public void setKafkaTime(TimeDuration kafkaTime) {
        this.kafkaTime = kafkaTime;
    }

    @Override
    public String toString() {
        return "RunningTime{" + "getTime=" + getTime + ", putTime=" + putTime + ", kafkaTime=" + kafkaTime + '}';
    }
}
