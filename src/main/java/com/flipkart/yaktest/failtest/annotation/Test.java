package com.flipkart.yaktest.failtest.annotation;

import com.flipkart.yaktest.failtest.models.TestCaseName;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Test {

    TestCaseName name() default TestCaseName.BLANK;

}
