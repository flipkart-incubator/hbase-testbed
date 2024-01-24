package com.flipkart.yaktest.failtest

import com.codahale.metrics.MetricRegistry
import com.flipkart.yaktest.failtest.dao.YakStore
import com.flipkart.yaktest.failtest.kafka.KafkaVerifierService
import com.flipkart.yaktest.output.TestOutput
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.powermock.reflect.Whitebox
import org.spockframework.runtime.Sputnik
import spock.lang.Specification

import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import static com.flipkart.yaktest.failtest.models.TestCaseName.PUT_GET_KAFKA_TEST
import static com.flipkart.yaktest.failtest.models.TestCaseName.PUT_GET_TEST

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([YakTest.class])
@Ignore
class YakTestSpec extends Specification {

    YakStore yakStore
    YakTest yakTest
    ExecutorService executorService

    void setup() {
        def dataSize = 1
        def runSize = 1
        AtomicInteger atomicInteger = Mock()
        yakStore = Mock()
        executorService = Executors.newFixedThreadPool(1)
        WriteRunner writeRunner = new WriteRunner(dataSize, runSize, yakStore, atomicInteger, 1, null)

        PowerMockito.whenNew(YakStore.class).withAnyArguments().thenReturn(yakStore)
        PowerMockito.whenNew(WriteRunner.class).withAnyArguments().thenReturn(writeRunner)
    }

    @Test
    def "test case 'putGetTest'"() {
        given:
            def concc = 1
            yakTest = new YakTest(concc, repeats)
            Whitebox.setInternalState(yakTest, "writeExec", executorService)
        when:
            yakTest.putGetTest()
        then:
            TestOutput.INSTANCE.getTestCaseName() == PUT_GET_TEST
        then:
            for(int version=0; version<repeats; version++) {
                1 * yakStore.checkPut(_ as String, version+1, version)
                2 * yakStore.verifyGet(_ as String, version+1)
            }
        then:
            1 * yakStore.shutDown()
        where:
            repeats | _
            1       | _
            2       | _
            3       | _
    }

    @Test
    def "test case 'putGetKafkaTest'"() {
        given:
            def concc = 1
        KafkaVerifierService kafkaConsumer = Mock()
            PowerMockito.whenNew(KafkaVerifierService.class).withNoArguments().thenReturn(kafkaConsumer)

            yakTest = new YakTest(concc, repeats)
            Whitebox.setInternalState(yakTest, "writeExec", executorService)
        when:
            yakTest.putGetKafkaTest()
        then:
            TestOutput.INSTANCE.getTestCaseName() == PUT_GET_KAFKA_TEST
        then:
            1 * kafkaConsumer.consume()
        then:
            for(int version=0; version<repeats; version++) {
                1 * yakStore.checkPut(_ as String, version+1, version)
                2 * yakStore.verifyGet(_ as String, version+1)
            }
        then:
            1 * yakStore.shutDown()
        then:
            1 * kafkaConsumer.awaitReplicationLag()
        then:
            1 * kafkaConsumer.verifyData(_ as ConcurrentMap<String, Integer>)
        then:
            1 * kafkaConsumer.shutdown()
        where:
            repeats | _
            1       | _
            2       | _
            3       | _
    }
}