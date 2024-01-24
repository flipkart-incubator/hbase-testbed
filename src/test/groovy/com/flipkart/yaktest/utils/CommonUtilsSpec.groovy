package com.flipkart.yaktest.utils

import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.flipkart.yaktest.utils.CommonUtils.shutdownExecutor

class CommonUtilsSpec extends Specification {

    def "shutdownExecutor() call shutdown the submitted service"() {
        given: "submit two random ExecutorService"
            ExecutorService service1 = Executors.newFixedThreadPool(2)
            ExecutorService service2 = Executors.newFixedThreadPool(2)

            submitService(service1)
            submitService(service2)

        when: "shutdownExecutor() is called on first service"
            shutdownExecutor(service1)
        then: "only first service is terminated"
            service1.isTerminated() == true
            service2.isTerminated() == false

        when: "shutdownExecutor() is called on second service"
            shutdownExecutor(service2)
        then: "both the services are terminated now"
            service1.isTerminated() == true
            service2.isTerminated() == true
    }

    void submitService(ExecutorService service) {
        service.submit(new Callable<String>() {
            @Override
            String call() throws Exception {
                return null
            }
        })
    }
}
