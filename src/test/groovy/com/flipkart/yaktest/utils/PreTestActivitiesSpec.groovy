package com.flipkart.yaktest.utils

import com.flipkart.yaktest.Config
import com.flipkart.yaktest.ProgramArguments
import com.flipkart.yaktest.WALIsolationChecker
import com.flipkart.yaktest.failtest.kafka.KafkaVerifierService
import com.flipkart.yaktest.interruption.models.SSHConfig
import com.flipkart.yaktest.interruption.models.YakComponent
import com.flipkart.yaktest.interruption.utils.YakUtils
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.spockframework.runtime.Sputnik
import spock.lang.Specification

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([PreTestActivities.class, YakUtils.class, WALIsolationChecker.class, FileUtils.class])
@PowerMockIgnore(["javax.net.ssl.*","javax.management.*","com.jcraft.jsch.*"])
@Ignore
class PreTestActivitiesSpec extends Specification {

    @Test
    @Ignore
    def "methods call before tests execution in PreTestActivities class"() {
        given:
            String[] args = progArgs
            ProgramArguments programArguments = new ProgramArguments(args)

            YakComponent yakComponent = YakComponent.NAME_NODE
            Config config = Config.getInstance()

            def configList = ["test1", "test2"]
            Map <YakComponent, List <String>> testYakConfig = new HashMap<>()
            testYakConfig.put(yakComponent, configList)

            PowerMockito.mockStatic(FileUtils.class)

            KafkaVerifierService kafkaConsumer = Mock()
            PowerMockito.whenNew(KafkaVerifierService.class).withNoArguments().thenReturn(kafkaConsumer)

            Map<String, List<String>> testYakComponentConfig = new HashMap<>()
            testYakComponentConfig.put("testKey", configList)
            // config.setYakComponentConfig(testYakComponentConfig)

            PowerMockito.mockStatic(YakUtils.class)
            PowerMockito.doNothing().when(YakUtils.class, "createNamespace", SSHConfig.getInstance(), "zoneAA")
            PowerMockito.doNothing().when(YakUtils.class, "dropAndCreateTable", SSHConfig.getInstance(), false, "zoneAA")

            PowerMockito.mockStatic(WALIsolationChecker.class)
            PowerMockito.doNothing().when(WALIsolationChecker.class,"updatePreviousWalFilesInfo")
        when:
            PreTestActivities.doWork(programArguments)

            PowerMockito.verifyStatic(YakUtils.class)
            YakUtils.CREATE_NAMESPACE(Mockito.isA(SSHConfig.class), Mockito.isA(String.class))
            YakUtils.dropAndCreateTable(Mockito.isA(SSHConfig.class), Mockito.isA(Boolean), Mockito.isA(String.class))
            YakUtils.enableKafkaReplication(Mockito.isA(SSHConfig.class))

            PowerMockito.verifyStatic(WALIsolationChecker.class)
            WALIsolationChecker.updatePreviousWalFilesInfo()
        then:
            notThrown(Exception)
        where:
            progArgs                                          | _
            ["-interruptions", "none"]                        | _
            ["-interruptions", "none", "-disableReplication"] | _
    }
}