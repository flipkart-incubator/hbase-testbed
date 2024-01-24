package com.flipkart.yaktest.utils;

import com.flipkart.yaktest.failtest.annotation.Test;
import com.flipkart.yaktest.failtest.models.TestCaseName;
import com.flipkart.yaktest.interruption.annotation.Interruption;
import com.flipkart.yaktest.interruption.models.InterruptionName;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static com.flipkart.yaktest.interruption.models.InterruptionName.BLANK;

public class ReflectionUtils {

    public static Map<String, Method> loadTestMethodMap(Class<?> clazz) {
        Map<String, Method> testMethodMap = new HashMap<>();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Test.class)) {
                testMethodMap.put(getTestAnnName(method), method);
            }
        }
        return testMethodMap;
    }

    public static Map<String, Method> loadInterruptionMethodMap(Class<?> clazz) {
        Map<String, Method> interruptionMethodMap = new HashMap<>();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Interruption.class)) {
                interruptionMethodMap.put(getInterrAnnName(method), method);
            }
        }
        return interruptionMethodMap;
    }

    private static String getTestAnnName(Method method) {
        TestCaseName annotationValue = method.getAnnotation(Test.class).name();
        return annotationValue == TestCaseName.BLANK ? method.getName() : annotationValue.toString();
    }

    private static String getInterrAnnName(Method method) {
        InterruptionName annotationValue = method.getAnnotation(Interruption.class).name();
        return annotationValue == BLANK ? method.getName() : annotationValue.getName();
    }
}
