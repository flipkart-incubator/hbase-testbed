package com.flipkart.yaktest.failtest

import com.flipkart.yak.client.exceptions.StoreDataNotFoundException
import com.flipkart.yak.distributor.KeyDistributor
import com.flipkart.yaktest.failtest.dao.Store
import com.flipkart.yaktest.failtest.exception.DataMismatchException
import com.flipkart.yaktest.failtest.utils.LogHelper
import com.flipkart.yaktest.output.HbaseOutputMetricName
import com.flipkart.yaktest.output.OutputMetricResult
import com.flipkart.yaktest.output.TestOutput
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

class WriteRunnerSpec extends Specification {

    void setup() {

    }

    def "write success test"() {
        given:
            KeyDistributor keyDistributor = Stub()
            LogHelper.INSTANCE.setup(keyDistributor)
            AtomicInteger atomicInteger = new AtomicInteger(0);
            Store store = Mock()
            WriteRunner writeRunner = new WriteRunner(dataSize,runSize,store, atomicInteger, 1, null);
        when:
            writeRunner.call()
        and:
            keyDistributor.enrichKey(_) >> new String("dummy")
        then:
            runSize * store.checkPut(_, _, _, _) >> ret
            atomicInteger.get() == runSize
            reconCount * store.verifyGet(_,_,_,_)
        where:
        runSize | dataSize | ret | reconCount
        1 | 1 | false | 1
        2 | 2 | false | 2
        1 | 1 | true | 0
    }

    def "write complete fail test"() {
        given:
            KeyDistributor keyDistributor = Stub()
            LogHelper.INSTANCE.setup(keyDistributor)
            AtomicInteger atomicInteger = new AtomicInteger(0);
            Store store = Stub()
            WriteRunner writeRunner = new WriteRunner(dataSize,runSize,store, atomicInteger, 1, null);
        when:
            TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().put(metric,new OutputMetricResult(metric, false))
            keyDistributor.enrichKey(_) >> new String("dummy")
            store.verifyGet(_,_,_,_) >> {throw e}
            store.checkPut(_, _, _, _) >> false
        and:
            writeRunner.call()
        then:
            atomicInteger.get() == 0
            TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(metric).getFailureCount().get() == 7*output
        where:
            runSize | dataSize | e | output | metric
            1 | 1 | new DataMismatchException(DataMismatchException.Type.READ_VERSION_HIGHER, "dummy") |1 | HbaseOutputMetricName.CHECK_PUT_FAIL
            1 | 1 | new StoreDataNotFoundException() |1 | HbaseOutputMetricName.CHECK_PUT_FAIL
    }
}
