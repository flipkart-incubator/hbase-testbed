package com.flipkart.yaktest.failtest

import com.flipkart.yak.client.exceptions.StoreDataNotFoundException
import com.flipkart.yak.distributor.KeyDistributor
import com.flipkart.yaktest.failtest.dao.Store
import com.flipkart.yaktest.failtest.exception.DataMismatchException
import com.flipkart.yaktest.failtest.utils.LogHelper
import com.flipkart.yaktest.output.HbaseOutputMetricName
import com.flipkart.yaktest.output.TestOutput
import org.apache.commons.lang.RandomStringUtils
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException

class GetVerifyRunnerSpec extends  Specification {
    GetVerifyRunner getVerifyRunnerUnderTest;
    Map<String, Integer> idVersionMap;
    String routeKey = "DUMMY";
    void setup() {
        idVersionMap = new HashMap<>();
    }

    private void idVersionMapPopulator(int count) {
        String charset = (('A'..'Z') + ('0'..'9')).join()
        Integer length = 9
        this.idVersionMap.clear()
        while(count-- > 0) {
            String randomString = RandomStringUtils.random(length, charset.toCharArray())
            this.idVersionMap.put(randomString,1);
        }
    }

    def "get verify Exception test"() {
        given:
            Store store = Stub()
            KeyDistributor keyDistributor = Stub()
            LogHelper.INSTANCE.setup(keyDistributor)
            CountDownLatch countDownLatch = new CountDownLatch(1);
            getVerifyRunnerUnderTest = new GetVerifyRunner(countDownLatch, idVersionMap, store, routeKey, false );
        when:
            idVersionMapPopulator(inputCount);
            keyDistributor.enrichKey(_) >> new String("dummy")
            store.verifyGet(_, _, _, _) >> {throw exception}
        and:
            getVerifyRunnerUnderTest.run()
        then:
            TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(metric).getFailureCount().get() == output
        where:
            output| inputCount | exception                                                                           | metric
            1     |   1        | new DataMismatchException(DataMismatchException.Type.READ_VERSION_HIGHER, "dummy")  |HbaseOutputMetricName.DATA_MISMATCH_RH
            2     |   2        | new DataMismatchException(DataMismatchException.Type.WRITE_VERSION_HIGHER, "dummy") |HbaseOutputMetricName.DATA_MISMATCH_WH
            1     |   1        | new StoreDataNotFoundException()                                                    |HbaseOutputMetricName.GET_FAIL
            1     |   1        | new ExecutionException(new StoreDataNotFoundException())                            |HbaseOutputMetricName.DATA_LOSS
    }

}
