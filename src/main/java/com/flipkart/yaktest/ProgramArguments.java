package com.flipkart.yaktest;

import java.util.HashMap;
import java.util.Map;

public class ProgramArguments {

    private String[] args = null;
    public static final String REPLICA_STORE = "-replicaZone";

    private Map<String, Integer> switchIndexes = new HashMap<>();

    public ProgramArguments(String[] args) {
        parse(args);
    }

    private void parse(String[] arguments) {
        this.args = arguments;
        switchIndexes.clear();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                switchIndexes.put(args[i], i);
            }
        }
    }

    public String[] args() {
        return args;
    }

    public String arg(int index) {
        return args[index];
    }

    public boolean switchPresent(String switchName) {
        return switchIndexes.containsKey(switchName);
    }

    public String switchValue(String switchName) {
        return switchValue(switchName, null);
    }

    public String switchValue(String switchName, String defaultValue) {
        if (!switchIndexes.containsKey(switchName)) return defaultValue;

        int switchIndex = switchIndexes.get(switchName);
        if (switchIndex + 1 < args.length) {
            return args[switchIndex + 1];
        }
        return defaultValue;
    }

    public int switchIntValue(String switchName, int defaultValue) {
        String switchValue = switchValue(switchName, null);

        if (switchValue == null) return defaultValue;
        return Integer.parseInt(switchValue);
    }

    public String[] switchValues(String switchName) {
        if (!switchIndexes.containsKey(switchName)) return new String[0];

        int switchIndex = switchIndexes.get(switchName);

        int nextArgIndex = switchIndex + 1;
        while (nextArgIndex < args.length && !args[nextArgIndex].startsWith("-")) {
            nextArgIndex++;
        }

        String[] values = new String[nextArgIndex - switchIndex - 1];
        for (int j = 0; j < values.length; j++) {
            values[j] = args[switchIndex + j + 1];
        }
        return values;
    }
}
