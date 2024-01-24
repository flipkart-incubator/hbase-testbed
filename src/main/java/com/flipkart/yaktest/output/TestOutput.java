package com.flipkart.yaktest.output;

import com.flipkart.yaktest.failtest.models.TestCaseName;
import com.flipkart.yaktest.interruption.models.InterruptionName;
import com.flipkart.yaktest.interruption.models.YakComponent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestOutput {

    public static final TestOutput INSTANCE = new TestOutput();

    private TestCaseName testCaseName;
    private TestStatus testStatus = new TestStatus();
    private RunningTime runningTime = new RunningTime();
    private ConcurrentMap<InterruptionName, InterruptionStatus> interruptionsStatus = new ConcurrentHashMap<>();
    private Map<YakComponent, List<ProcessStatus>> processesStatus;
    private AtomicBoolean isInterruptionParallel = new AtomicBoolean(false);

    public ConcurrentMap<InterruptionName, InterruptionStatus> getInterruptionsStatus() {
        return interruptionsStatus;
    }

    public void setTestCaseName(TestCaseName testCaseName) {
        this.testCaseName = testCaseName;
    }

    public TestStatus getTestStatus() {
        return testStatus;
    }

    public Map<YakComponent, List<ProcessStatus>> getProcessesStatus() {
        return processesStatus;
    }

    public AtomicBoolean getIsInterruptionParallel() {
        return isInterruptionParallel;
    }

    public TestCaseName getTestCaseName() {
        return testCaseName;
    }

    public void setTestStatus(TestStatus testStatus) {
        this.testStatus = testStatus;
    }

    public void setInterruptionsStatus(ConcurrentMap<InterruptionName, InterruptionStatus> interruptionsStatus) {
        this.interruptionsStatus = interruptionsStatus;
    }

    public void setProcessesStatus(Map<YakComponent, List<ProcessStatus>> processesStatus) {
        this.processesStatus = processesStatus;
    }

    public void setIsInterruptionParallel(AtomicBoolean isInterruptionParallel) {
        this.isInterruptionParallel = isInterruptionParallel;
    }

    public RunningTime getRunningTime() {
        return runningTime;
    }

    @Override
    public String toString() {
        return "TestOutput{" + "testCaseName=" + testCaseName + ", testStatus=" + testStatus + ", runningTime=" + runningTime + ", interruptionsStatus="
                + interruptionsStatus + ", processesStatus=" + processesStatus + ", isInterruptionParallel=" + isInterruptionParallel + '}';
    }
}
