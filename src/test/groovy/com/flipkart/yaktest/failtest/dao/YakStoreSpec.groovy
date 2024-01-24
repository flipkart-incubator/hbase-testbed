package com.flipkart.yaktest.failtest.dao

import com.flipkart.yak.client.AsyncStoreClient
import com.flipkart.yak.client.pipelined.config.MultiZoneStoreConfig
import com.flipkart.yak.distributor.KeyDistributor
import com.flipkart.yak.models.Cell
import com.flipkart.yak.models.CheckAndStoreData
import com.flipkart.yak.models.ColumnsMap
import com.flipkart.yak.models.GetColumnMap
import com.flipkart.yak.models.StoreData
import com.flipkart.yaktest.failtest.exception.DataMismatchException
import com.flipkart.yaktest.failtest.models.Data
import com.flipkart.yaktest.failtest.utils.LogHelper
import org.apache.hadoop.hbase.util.Bytes
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

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Sputnik.class)
@PrepareForTest([YakStore.class])
@Ignore
class YakStoreSpec extends Specification {

    YakStore yakStore
    AsyncStoreClient client
    KeyDistributor keyDistributor
    MultiZoneStoreConfig config

    void setup() {
        keyDistributor = Mock()
        config = Mock()
    }

    void customSetup() {
        client = Mock() {
            put(_ as StoreData) >> {}
            batchPut(_ as List<StoreData>) >> {}
            batchPut(_ as CheckAndStoreData) >> {}
        }
        PowerMockito.whenNew(AsyncStoreClient.class).withAnyArguments().thenReturn(client)
        yakStore = new YakStore(config, "testTable", keyDistributor)
    }

    @Test
    def "YakStore put()"() {
        given:
            customSetup()
            def testKey = "testKey"
            def testVersion = 1
        when:
            yakStore.put(testKey, testVersion)
        then:
            1 * client.put(_ as StoreData)
    }

    @Test
    def "YakStore batchPut()"() {
        given:
            customSetup()
            Map<String, Integer> keyVersionMap = [testKey: 1]
        when:
            yakStore.batchPut(keyVersionMap)
        then:
            1 * client.batchPut(_ as List<StoreData>)
    }

    @Test
    def "YakStore checkPut()"() {
        given:
            customSetup()
            def testKey = "testKey"
            def putVersion = 1
            def expectedVersion = 1
        when:
            yakStore.checkPut(testKey, putVersion, expectedVersion)
        then:
            1 * client.put(_ as CheckAndStoreData)
    }

    @Test
    def "valid case for YakStore verifyGet()"() {
        given:
            def testKey = "testKey"
            ColumnsMap map = new ColumnsMap()
            addCellColumnsMap(map, "instance", testInstanceValue)
            addCellColumnsMap(map, "version", testVersion)

            MultiZoneStoreConfig config = Mock()
            KeyDistributor keyDistributor = Mock()

            client = Stub() {
                get(_ as GetColumnMap) >> map
            }

            Whitebox.setInternalState(LogHelper.INSTANCE, "distributor", new KeyDistributor() {
                @Override
                byte[] partitionHint(byte[] bytes) {
                    return new byte[0]
                }
            })

            Data.INSTANCE.snapshotLoad = snapshotLoadValue

            PowerMockito.whenNew(StoreClientImpl.class).withAnyArguments().thenReturn(client)
            yakStore = new YakStore(config, "testTable", keyDistributor)
        when:
            yakStore.verifyGet(testKey, expectedVersion)
        then:
            notThrown(Exception)
        where:
            testInstanceValue | snapshotLoadValue   | testVersion | expectedVersion
            "testValue"       | "testValue"         | 1           | 1
    }

    @Test
    def "data mismatch cases for YakStore verifyGet() "() {
        given:
            def testKey = "testKey"
            ColumnsMap map = new ColumnsMap()
            addCellColumnsMap(map, "instance", testInstanceValue)
            addCellColumnsMap(map, "version", testVersion)

            MultiZoneStoreConfig config = Mock()
            KeyDistributor keyDistributor = Mock()

            client = Stub() {
                get(_ as GetColumnMap) >> map
            }

            Whitebox.setInternalState(LogHelper.INSTANCE, "distributor", new KeyDistributor() {
                @Override
                byte[] partitionHint(byte[] bytes) {
                    return new byte[0]
                }
            })

            Data.INSTANCE.snapshotLoad = snapshotLoadValue

            PowerMockito.whenNew(StoreClientImpl.class).withAnyArguments().thenReturn(client)
            yakStore = new YakStore(config, "testTable", keyDistributor)
        when:
            yakStore.verifyGet(testKey, expectedVersion)
        then:
            thrown(DataMismatchException)
        where:
            testInstanceValue | snapshotLoadValue   | testVersion | expectedVersion
            "testValue"       | "modifiedTestValue" | 1           | 1
            "testValue"       | "testValue"         | 0           | 1
            "testValue"       | "modifiedTestValue" | 0           | 1
    }

    void addCellColumnsMap (ColumnsMap map, String key, def testValue) {
        byte[] randomBytes = Bytes.toBytes(testValue)
        Cell cell = new Cell(randomBytes)
        map.put(key, cell)
    }
}