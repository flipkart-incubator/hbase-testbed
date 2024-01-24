package com.flipkart.yaktest

import com.flipkart.yak.client.AsyncStoreClient
import com.flipkart.yaktest.failtest.dao.YakStore
import com.flipkart.yaktest.interruption.models.SSHConfig
import com.flipkart.yaktest.interruption.utils.YakUtils
import com.flipkart.yaktest.models.BlockStatus
import com.flipkart.yaktest.models.RegionInfo
import com.flipkart.yaktest.output.HbaseOutputMetricName
import com.flipkart.yaktest.output.OutputMetricResult
import com.flipkart.yaktest.output.Status
import com.flipkart.yaktest.output.TestOutput
import com.flipkart.yaktest.output.TestStatus
import com.flipkart.yaktest.utils.FileUtils
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.modules.junit4.PowerMockRunnerDelegate
import org.powermock.reflect.Whitebox
import org.spockframework.runtime.Sputnik
import spock.lang.Shared
import spock.lang.Specification

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([WALIsolationChecker.class, YakStore.class, PostTestActivities.class, YakUtils.class, HBlockIsolationChecker.class, FileUtils.class])
@PowerMockIgnore(["javax.net.ssl.*","javax.management.*","com.jcraft.jsch.*"])
@Ignore
class PostTestActivitiesSpec extends Specification {

    @Shared
    ProgramArguments programArguments

    def setupSpec() {
        def mockedInconsistencyString = "0"
        def testRegion = new RegionInfo("testRegion", "startKey", "endKey")
        def mockedOpenRegionsList = ["test1", "test2"]
        def fetchedRegionsInfo = [test1: testRegion, test2: testRegion]

        String[] args = ["-interruptions", "none"]
        programArguments = new ProgramArguments(args)

        PowerMockito.mockStatic(FileUtils.class)

        YakStore yakStore = Mock()
        AsyncStoreClient storeClient = Mock()

        BlockStatus blockStatus = new BlockStatus()
        blockStatus.setOkBlockCount(1)
        blockStatus.setTotalBlockCount(1)
        blockStatus.setDescription("0 Wal files are in wrong group")

        PowerMockito.mockStatic(WALIsolationChecker.class)
        PowerMockito.when(WALIsolationChecker.getIsolationStatus()).thenReturn(blockStatus)

        PowerMockito.mockStatic(YakUtils.class)
        PowerMockito.when(YakUtils.checkInconsistencies(SSHConfig.getInstance())).thenReturn(mockedInconsistencyString)
        PowerMockito.when(YakUtils.fetchOpenStateRegions(SSHConfig.getInstance())).thenReturn(mockedOpenRegionsList)
        PowerMockito.when(YakUtils.fetchRegionsInfo(SSHConfig.getInstance())).thenReturn(fetchedRegionsInfo)

        PowerMockito.mockStatic(HBlockIsolationChecker.class)
        PowerMockito.when(HBlockIsolationChecker.getIsolationStatus()).thenReturn(blockStatus)

        PowerMockito.whenNew(YakStore.class).withAnyArguments().thenReturn(yakStore)
        Whitebox.setInternalState(yakStore, "storeClient", storeClient)

        PostTestActivities.doWork(programArguments)
    }

    @Test
    def "HBase inconsistency metric result"() {
        when:
        OutputMetricResult inconsistencyMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.INCONSISTENCIES)
        then:
        inconsistencyMetricResult.getFailureCount().get() == 0
        inconsistencyMetricResult.getResult() == false
        inconsistencyMetricResult.getStatus() == Status.PASSED
    }

    @Test
    def "HBase WAL Isolation metric result"() {
        when:
        OutputMetricResult walIsolationMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.WAL_ISOLATED)
        then:
        walIsolationMetricResult.getFailureCount().get() == 0
        walIsolationMetricResult.getStatus() == Status.PASSED
    }

    @Test
    def "HBlock Isolation metric result"() {
        when:
        OutputMetricResult hBlockIsolationMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.HBLOCK_ISOLATED)
        then:
        hBlockIsolationMetricResult.getFailureCount().get() == 0
        hBlockIsolationMetricResult.getStatus() == Status.PASSED
    }


    @Test
    def "HBase exceptions count after putGet in all regions"() {
        when:
        OutputMetricResult failuresAfterTestMetricResult = TestOutput.INSTANCE.getTestStatus().getHbaseMetrices().get(HbaseOutputMetricName.FAILURES_AFTER_TEST);
        then:
        failuresAfterTestMetricResult.getFailureCount().get() == 0
        failuresAfterTestMetricResult.getResult() == false
        failuresAfterTestMetricResult.getStatus() == Status.PASSED
    }

    @Test
    def "TestOutput overall status"() {
        when:
        TestStatus testStatus = TestOutput.INSTANCE.getTestStatus()
        then:
        testStatus.getOverallStatus() == Status.PASSED
    }

    @Test
    def "output log file creation is mocked and hence not created but FileUtils.writeOutputToFile was originally called"() {
        when:
        def outputFileName = "output.json"
        def outputFilePath = FileUtils.getLogPath()  + outputFileName
        File outputFile = new File(outputFilePath)
        then:
        assert !(outputFile.exists()) : "output file doesn't exist while testing"
        PowerMockito.verifyStatic(FileUtils.class)
        FileUtils.writeOutputToFile(TestOutput.INSTANCE)
    }
}