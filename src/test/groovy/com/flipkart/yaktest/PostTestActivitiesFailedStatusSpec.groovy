package com.flipkart.yaktest

import com.flipkart.yak.client.AsyncStoreClient
import com.flipkart.yaktest.failtest.dao.YakStore
import com.flipkart.yaktest.interruption.models.SSHConfig
import com.flipkart.yaktest.interruption.utils.YakUtils
import com.flipkart.yaktest.models.BlockStatus
import com.flipkart.yaktest.models.RegionInfo
import com.flipkart.yaktest.output.HbaseOutputMetricName
import com.flipkart.yaktest.output.SepOutputMetricName
import com.flipkart.yaktest.output.Status
import com.flipkart.yaktest.output.TestOutput
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
import spock.lang.Specification

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([WALIsolationChecker.class, YakStore.class, PostTestActivities.class, YakUtils.class, HBlockIsolationChecker.class])
@PowerMockIgnore(["javax.net.ssl.*","javax.management.*","com.jcraft.jsch.*"])
@Ignore
class PostTestActivitiesFailedStatusSpec extends Specification {

    ProgramArguments programArguments

    def setup() {
        def mockedInconsistencyString = "0"
        def testRegion = new RegionInfo("testRegion", "startKey", "endKey")
        def mockedOpenRegionsList = ["test1", "test2"]
        def fetchedRegionsInfo = [test1: testRegion, test2: testRegion]

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
        Whitebox.setInternalState(yakStore, "client", storeClient)
    }

    @Test
    def "FAILED overall status when a Hbase mismatch metric is set"() {
        given:
            String[] args = ["-interruptions", "none"]
            programArguments = new ProgramArguments(args)

            TestOutput testOutput = TestOutput.INSTANCE
            testOutput.getTestStatus().getHbaseMetrices().get(mismatchMetricValue).setResult(setValue)
        when:
            PostTestActivities.doWork(programArguments)
        then:
            testOutput.getTestStatus().getOverallStatus() == Status.FAILED
        where:
            mismatchMetricValue                    | setValue
            HbaseOutputMetricName.DATA_LOSS        | true
            HbaseOutputMetricName.DATA_MISMATCH_WH | true
            HbaseOutputMetricName.DATA_MISMATCH_RH | true
    }

    @Test
    def "FAILED overall status when a Kafka mismatch metric is set"() {
        given:
            String[] args = ["-interruptions", "none"]
            programArguments = new ProgramArguments(args)

            TestOutput testOutput = TestOutput.INSTANCE

            testOutput.getTestStatus().getSepMetrices().get(mismatchMetricValue).setResult(setValue)
        when:
            PostTestActivities.doWork(programArguments)
        then:
            testOutput.getTestStatus().getOverallStatus() == Status.FAILED
        where:
            mismatchMetricValue                  | setValue
            SepOutputMetricName.DATA_LOSS        | true
            SepOutputMetricName.DATA_MISMATCH_WH | true
        SepOutputMetricName.DATA_MISMATCH_RH     | true
            SepOutputMetricName.ORDERING         | false
    }
}