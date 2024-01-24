package com.flipkart.yaktest.failtest.kafka

import com.flipkart.yaktest.output.SepOutputMetricName
import com.flipkart.yaktest.output.TestOutput
import com.flipkart.yaktest.output.TestStatus
import org.junit.Test
import org.junit.runner.RunWith
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.powermock.reflect.Whitebox
import org.spockframework.runtime.Sputnik
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([KafkaVerifierService.class])
class KafkaVerifierServiceSpec extends Specification {

    @Shared
    ExecutorService kafkaExec
    @Shared
    KafkaVerifierService kafkaConsumer

    void setupSpec() {
        kafkaConsumer = new KafkaVerifierService()
        kafkaExec = Executors.newFixedThreadPool(2)
        Whitebox.setInternalState(kafkaConsumer, "kafkaExec", kafkaExec)
    }

     @spock.lang.Ignore
     @Test
     def "kafka consumer service startConsuming and shutdown"() {
         //This test needs to be updated to test startConsuming() and shutDown().
         //Gives some problem with KafkaHelper class mocking. Ignoring the test for now.
         when:
             kafkaConsumer.consume()
         then:
             kafkaExec.isTerminated() == false
         when:
            kafkaConsumer.shutdown()
         then:
            kafkaExec.isTerminated() == true
     }

    @Test
    def "repetitions for key via kafka replication"() {
        given:
            Map<String, Integer> randomData = new HashMap<>()
            randomData.put("randomKey1", 1)
            randomData.put("randomKey2", 2)

            def initialCapacity = 2
            ConcurrentMap<String, Integer> versionMap = new ConcurrentHashMap<>(initialCapacity)
            versionMap.putAll(randomData)

            ConcurrentMap <String, List<Integer>> allVersionsMap = new ConcurrentHashMap<>()
            allVersionsMap.put("allVersionsTestKey", allVersionsList)
            Whitebox.setInternalState(kafkaConsumer, "sepAllVersionsMap", allVersionsMap)
        when:
            kafkaConsumer.verifyData(versionMap)

            TestStatus testStatus = TestOutput.INSTANCE.getTestStatus()
            def kafkaMetricMap = testStatus.getSepMetrices()
        then:
            kafkaMetricMap.get(SepOutputMetricName.REPETITION).getResult() == repetitionsResult
        where:
            allVersionsList | repetitionsResult
            [1, 2, 3]       | false
            [1]             | false
            [1, 2, 3, 3, 3] | true
            [1, 2, 2]       | true
    }

    @Test
    def "data mismatch read ahead for key via kafka replication"() {
        given:
            Map<String, Integer> randomData = new HashMap<>()
            randomData.put(putVersionKey, putVersionValue)

            def initialCapacity = 2
            ConcurrentMap<String, Integer> versionMap = new ConcurrentHashMap<>(initialCapacity)
            versionMap.putAll(randomData)

            ConcurrentMap <String, List<Integer>> allVersionsMap = new ConcurrentHashMap<>()
            allVersionsMap.put(allVersionsKey, allVersionsList)
            Whitebox.setInternalState(kafkaConsumer, "sepAllVersionsMap", allVersionsMap)
        when:
            kafkaConsumer.verifyData(versionMap)

            TestStatus testStatus = TestOutput.INSTANCE.getTestStatus()
            def kafkaMetricMap = testStatus.getSepMetrices()
        then:
            kafkaMetricMap.get(SepOutputMetricName.DATA_MISMATCH_RH).getResult() == repetitionsResult

        where:
            putVersionKey | putVersionValue | allVersionsKey | allVersionsList | repetitionsResult
            "randomKey"   | 1               | "differentKey" | [1, 2]          | true
            "randomKey"   | 1               | "differentKey" | [1, 1]          | true
            "sameKey"     | 1               | "sameKey"      | [1, 2]          | true
    }
}