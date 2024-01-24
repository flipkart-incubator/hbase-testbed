package com.flipkart.yaktest.failtest.utils;

import org.apache.commons.lang3.RandomStringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public enum RandomUtil {
    INSTANCE;

    private final char USABLE_SET[] = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static Random random;
    private static Random rand;
    private static String host = "";
    public ConcurrentMap<String, Boolean> randomKeyMap = new ConcurrentHashMap<>();

    static {
        try {
            random = new Random(InetAddress.getLocalHost().getHostAddress().hashCode());
            rand = new Random();
            //uncomment if running from multiple host
            host = InetAddress.getLocalHost().getHostAddress().replace('.', '0');
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public String randomKey() {
        String val = RandomStringUtils.random(4, 0, 9, false, true, USABLE_SET, random);
        String r = val + host + System.nanoTime() + Thread.currentThread().getId();
        if (randomKeyMap.containsKey(r)) {
            return randomKey();
        }
        randomKeyMap.put(r, true);
        return r;
    }

    public int randomInt(int bound) {
        return rand.nextInt(bound);
    }

    public List<Integer> randomInts(int bound, int count) {

        List<Integer> randoms = new ArrayList<>();
        if (count >= bound / 2) {
            return IntStream.range(0, count).boxed().collect(Collectors.toList());
        }

        for (int i = 0; i < count; i++) {
            Integer rand = randomInt(bound);
            while (randoms.contains(rand)) {
                rand = randomInt(bound);
            }
            randoms.add(rand);
        }

        return randoms;
    }
}
