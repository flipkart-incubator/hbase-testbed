package com.flipkart.yaktest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.flipkart.yaktest.failtest.YakTest;
import com.flipkart.yaktest.interruption.InterruptionProvider;
import com.flipkart.yaktest.output.TestOutput;
import com.flipkart.yaktest.utils.CommonUtils;
import com.flipkart.yaktest.utils.PreTestActivities;
import com.flipkart.yaktest.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MainClass {

    private static Logger logger = LoggerFactory.getLogger(MainClass.class);
    private static final int defaultConcurrency = 2;
    private static final int defaultInterruptionDuration = 15000;
    private static final int defaultInterruptionGap = 2000;
    private static ExecutorService executorService;

    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME", "hbase");
            MetricRegistry registry = new MetricRegistry();
            JmxReporter reporter = JmxReporter.forRegistry(registry).build();
            reporter.start();

            ProgramArguments programArguments = new ProgramArguments(args);
            Map<String, Method> testMethodMap = ReflectionUtils.loadTestMethodMap(YakTest.class);
            Map<String, Method> interruptionMethodMap = ReflectionUtils.loadInterruptionMethodMap(InterruptionProvider.class);

            if (programArguments.args().length == 0 || programArguments.arg(0).equals("help")) {
                printUsage(testMethodMap, interruptionMethodMap);
            }

            logger.info("Test Started");

            PreTestActivities.doWork(programArguments, registry);

            //Interruption Part
            List<Future> interruptionFutures = submitInterruptions(programArguments, interruptionMethodMap);

            //Test Part
            executeTests(programArguments, testMethodMap, registry);

            waitForInterruptionsToFinish(interruptionFutures);

            PostTestActivities.doWork(programArguments, registry);

            logger.info(TestOutput.INSTANCE.toString());
        } catch (Exception ex) {
            logger.error("Failed with exception: {}", ex.getMessage(), ex);
        }
    }

    private static void waitForInterruptionsToFinish(List<Future> interruptionFutures) throws InterruptedException {
        interruptionFutures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        CommonUtils.shutdownExecutor(executorService);
    }

    private static List<Future> submitInterruptions(ProgramArguments programArguments, Map<String, Method> interruptionMethodMap) throws Exception {
        int interruptionDuration = programArguments.switchIntValue("-interruptionDuration", defaultInterruptionDuration);
        int interruptionGap = programArguments.switchIntValue("-interruptionGap", defaultInterruptionGap);
        InterruptionProvider interruptionProvider = new InterruptionProvider(interruptionDuration, interruptionGap);
        String[] interruptions = programArguments.switchValues("-interruptions");

        List<Future> futures = new ArrayList<>();
        if (!programArguments.switchPresent("-parallel")) {
            executorService = Executors.newFixedThreadPool(1);
            futures.add(executorService.submit(() -> {
                for (String interruption : interruptions) {
                    invokeInterruption(interruption, interruptionProvider, interruptionMethodMap);
                }
            }));
        } else {
            executorService = Executors.newFixedThreadPool(interruptions.length);
            TestOutput.INSTANCE.getIsInterruptionParallel().set(true);
            for (String interruption : interruptions) {
                futures.add(executorService.submit(() -> invokeInterruption(interruption, interruptionProvider, interruptionMethodMap)));
            }
        }
        return futures;
    }

    private static void executeTests(ProgramArguments programArguments, Map<String, Method> testMethodMap,
                                     MetricRegistry registry) throws Exception {
        int concurrency = programArguments.switchIntValue("-concc", defaultConcurrency);
        int repeats = programArguments.switchIntValue("-repeats", defaultConcurrency);

        YakTest yakTest = new YakTest(concurrency, repeats, registry);
        if (programArguments.switchPresent(ProgramArguments.REPLICA_STORE)) {
            yakTest.setRouteKey(programArguments.switchValue(ProgramArguments.REPLICA_STORE));
        }
        String testName = programArguments.switchValue("-test");

        logger.info("running test {}", testName);
        Method method = testMethodMap.get(testName);
        if (method == null) {
            logger.error("Invalid test name {} passed in parameters", testName);
            System.exit(-1);
        }
        method.invoke(yakTest);
    }

    private static void invokeInterruption(String interruption, InterruptionProvider interruptionProvider, Map<String, Method> interruptionMethodMap) {
        if ("none".equalsIgnoreCase(interruption)) {
            return;
        }
        try {
            String[] interruptionSpits = interruption.split("-");
            int count = 1;
            boolean isParallel = true;
            if (interruptionSpits.length > 1) {
                count = Integer.parseInt(interruptionSpits[1]);
            }

            if (interruptionSpits.length > 2) {
                isParallel = !interruptionSpits[2].trim().equals("serial");
            }

            Method interruptionMethod = interruptionMethodMap.get(interruptionSpits[0]);
            if (interruptionMethod == null) {
                logger.error("Invalid interruption {} passed in parameters", interruption);
                System.exit(-1);
            }
            interruptionMethodMap.get(interruptionSpits[0]).invoke(interruptionProvider, count, isParallel);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void printUsage(Map<String, Method> testMethodMap, Map<String, Method> interruptionMethodMap) {
        StringJoiner availableTests = new StringJoiner("|");
        testMethodMap.keySet().forEach(availableTests::add);

        StringJoiner availableInterruptions = new StringJoiner("|");
        interruptionMethodMap.keySet().forEach(availableInterruptions::add);

        String USAGE =
                "java -cp <target_jar> com.flipkart.yak.MainClass -test <test> -concc <concc> -repeats <repeats> -interruptions <interruptions> -interruptionDuration <duration(ms)> -interruptionGap <gap(ms)> -parallel";
        String usage = USAGE.replace("<test>", availableTests.toString()).replace("<interruptions>", availableInterruptions.toString());
        logger.info(usage);
        System.exit(0);
    }
}