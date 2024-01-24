package com.flipkart.yaktest.failtest.dao;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.AsyncStoreClient;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.MasterSlaveYakPipelinedStoreImpl;
import com.flipkart.yak.client.pipelined.SyncYakPipelinedStore;
import com.flipkart.yak.client.pipelined.SyncYakPipelinedStoreImpl;
import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.config.MultiZoneStoreConfig;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreDataCorruptException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.route.HotRouter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.*;
import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.failtest.exception.DataMismatchException;
import com.flipkart.yaktest.failtest.models.Data;
import com.flipkart.yaktest.failtest.utils.LogHelper;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class YakStore implements Store {

    private static Logger logger = LoggerFactory.getLogger(YakStore.class);

    private final PipelineConfig config;
    private final StoreRoute route;
    private final YakPipelinedStore store;
    private final AsyncStoreClient storeClient;
    private final SyncYakPipelinedStore syncStore;
    private final String tableName;
    private final String tableNameBackup;
    private final KeyDistributor keyDistributor;
    private final Map<String, SiteId> sites = new HashMap<>();
    private final HystrixSettings hystrixSettings;

    private final HotRouter<MasterSlaveReplicaSet, String> hotRouter = new HotRouter<MasterSlaveReplicaSet, String>() {
        @Override
        public MasterSlaveReplicaSet getReplicaSet(Optional<String> routeKey)
                throws NoSiteAvailableToHandleException, PipelinedStoreDataCorruptException {
            if (routeKey.isPresent() && sites.containsKey(routeKey.get())) {
                return new MasterSlaveReplicaSet(sites.get(routeKey.get()), new ArrayList<>());
            } else {
                return new MasterSlaveReplicaSet(sites.get(Config.getInstance().getDefaultSite()), new ArrayList<>());
            }
        }
    };

    public YakStore(String tableName, String tableNameBackup, MultiZoneStoreConfig multiZoneStoreConfig,
                    MetricRegistry registry) throws Exception {
        this(multiZoneStoreConfig, tableName, tableNameBackup, new MurmusHashDistribution(90), registry);
    }

    public YakStore(String tableName, String tableNameBackup, MultiZoneStoreConfig multiZoneStoreConfig,
                    KeyDistributor keyDistributor, MetricRegistry registry) throws Exception {
        this(multiZoneStoreConfig, tableName, tableNameBackup, keyDistributor, registry);
    }

    public YakStore(MultiZoneStoreConfig multiZoneStoreConfig, String tableName, String tableNameBackup,
                    KeyDistributor keyDistributor, MetricRegistry registry) throws Exception {
        HystrixObservableCommand.Setter storeSettings =
                HystrixObservableCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("hystrix"));
        storeSettings
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(200));
        hystrixSettings = new HystrixSettings(storeSettings);

        for (Zone zone : multiZoneStoreConfig.getZones().keySet()) {
            for (String siteName : multiZoneStoreConfig.getZones().get(zone).getSites().keySet()) {
                SiteConfig site = multiZoneStoreConfig.getZones().get(zone).getSite(siteName).get();
                sites.putIfAbsent(site.getStoreName(), new SiteId(siteName, zone));
            }
        }
        Optional<Map<String, KeyDistributor>> keyDistributorMap = Optional.of(new HashMap<>());
        keyDistributorMap.get().put(tableName, keyDistributor);
        keyDistributorMap.get().put(tableNameBackup, keyDistributor);
        this.route =
                new StoreRoute(Zone.IN_CHENNAI_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_MANDATORY,
                        hotRouter);
        this.config = new PipelineConfig(multiZoneStoreConfig, 30, 20, "test-bed-client", keyDistributorMap);
        this.keyDistributor = keyDistributor;
        this.tableName = tableName;
        this.tableNameBackup = tableNameBackup;
        this.store = new MasterSlaveYakPipelinedStoreImpl(config, route, registry);
        this.storeClient = (AsyncStoreClient) this.store.getAsyncStoreClient(Optional.empty()).get(0);
        this.syncStore = new SyncYakPipelinedStoreImpl(store);
    }

    @Override
    public void walRoll(Optional<String> routeKey) throws Exception {
        AsyncAdmin admin = this.storeClient.getConnection().getAdmin();

        Collection<ServerName> servers = admin.getRegionServers().get(60, TimeUnit.SECONDS);
        List<ServerName> deadServers = admin.listDeadServers().get(60, TimeUnit.SECONDS);

        for (ServerName server : servers) {
            if (!deadServers.contains(server)) {
                logger.info("Rolling server {}", server);
                admin.rollWALWriter(server).get(60, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void put(String key, int version, Optional<String> routeKey) throws Exception {
        StoreData data = new StoreDataBuilder(tableName).withRowKey(key.getBytes()).withDurability(Durability.FSYNC_WAL)
                .addColumn("data", "instance", (Data.INSTANCE.snapshotLoad).getBytes())
                .addColumn("data", "version", Integer.toString(version).getBytes())
                .addColumn("state", "keyvaltest", Integer.toString(version).getBytes()).build();
        syncStore.put(data, routeKey, Optional.empty(), Optional.of(hystrixSettings));
    }

    @Override
    public void batchPut(Map<String, Integer> keyVersionMap, Optional<String> routeKey) throws Exception {
        List<StoreData> storeDataList = keyVersionMap.entrySet().stream().map(
                        entry -> new StoreDataBuilder(tableName).withRowKey((entry.getKey()).getBytes())
                                .withDurability(Durability.FSYNC_WAL).addColumn("data", "instance", (Data.INSTANCE.snapshotLoad).getBytes())
                                .addColumn("data", "version", Integer.toString(entry.getValue()).getBytes())
                                .addColumn("state", "keyvaltest", Integer.toString(entry.getValue()).getBytes()).build())
                .collect(Collectors.toList());

        syncStore.put(storeDataList, routeKey, Optional.empty(), Optional.of(hystrixSettings));
    }

    @Override
    public boolean checkPut(String key, int version, int expected, Optional<String> routeKey) throws Exception {
        CheckAndStoreData data =
                new StoreDataBuilder(tableName).withRowKey((key.getBytes())).withDurability(Durability.FSYNC_WAL)
                        .addColumn("data", "instance", (Data.INSTANCE.snapshotLoad).getBytes())
                        .addColumn("data", "version", Integer.toString(version).getBytes())
                        .addColumn("state", "keyvaltest", Integer.toString(version).getBytes())
                        .buildWithCheckAndVerifyColumn("data", "version",
                                expected == 0 ? "".getBytes() : Integer.toString(expected).getBytes());

        PipelinedResponse<StoreOperationResponse<Boolean>> response =
                syncStore.checkAndPut(data, routeKey, Optional.empty(), Optional.of(hystrixSettings));

        Throwable error = response.getOperationResponse().getError();
        if (error != null) {
            if (error instanceof StoreDataNotFoundException || error.getCause() instanceof StoreDataNotFoundException) {
                throw new StoreDataNotFoundException();
            } else {
                throw new StoreException(error);
            }
        }
        return response.getOperationResponse().getValue();
    }

    @Override
    public void verifyGet(String key, Integer expectedVersion, Optional<String> routeKey, boolean queryFromBackup)
            throws Exception {
        String tableToQuery = (queryFromBackup) ? tableNameBackup : tableName;
        GetColumnMap getCol = new GetDataBuilder(tableToQuery).withRowKey((key).getBytes()).buildForColFamily("data");
        ;
        PipelinedResponse<StoreOperationResponse<ResultMap>> response =
                syncStore.get(getCol, routeKey, Optional.empty(), Optional.of(hystrixSettings));

        Throwable error = response.getOperationResponse().getError();
        if (error != null) {
            if (error instanceof StoreDataNotFoundException || error.getCause() instanceof StoreDataNotFoundException) {
                logger.info("Data not found with table: {}", getCol.getTableName());
                throw new StoreDataNotFoundException();
            } else {
                throw new StoreException(error);
            }
        }
        ColumnsMap map = response.getOperationResponse().getValue().get("data");
        byte[] instance = map.get("instance").getValue();
        byte[] version = map.get("version").getValue();

        String message = "";
        DataMismatchException.Type mismatchType = null;
        if (!Data.INSTANCE.snapshotLoad.equals(new String(instance))) {
            message += "payload compared failed for key " + LogHelper.INSTANCE.hbaseKey(key);
            mismatchType = DataMismatchException.Type.PAYLOAD_MISMATCH;
        }
        int actual = Integer.parseInt(Bytes.toString(version));

        Pair<DataMismatchException.Type, String> mismatch = checkVersionMismatch(actual, expectedVersion, key);
        message += mismatch.getSecond();
        mismatchType = mismatch.getFirst();

        if (mismatchType != null) {
            throw new DataMismatchException(mismatchType, message);
        }
    }

    @Override
    public void verifyGet(String key, Integer expectedVersion, Optional<String> routeKey, boolean queryFromBackup, int expectedTotalVersions)
            throws Exception {
        String tableToQuery = (queryFromBackup) ? tableNameBackup : tableName;
        GetRow getRow = new GetDataBuilder(tableToQuery).withRowKey((key).getBytes()).withMaxVersions(expectedTotalVersions).build();
        PipelinedResponse<StoreOperationResponse<ResultMap>> pipelinedResponse = syncStore.get(getRow, routeKey, Optional.empty(), Optional.of(hystrixSettings));

        Throwable error = pipelinedResponse.getOperationResponse().getError();
        if (error != null) {
            if (error instanceof StoreDataNotFoundException || error.getCause() instanceof StoreDataNotFoundException) {
                logger.info("Data not found with table: {}", getRow.getTableName());
                throw new StoreDataNotFoundException();
            } else {
                throw new StoreException(error);
            }
        }

        ResultMap resultMap = pipelinedResponse.getOperationResponse().getValue();
        ColumnsMap columnsMap = resultMap.get("data");
        int actualTotalVersions = columnsMap.get("version").getVersionedValues().size();
        String message = "";
        DataMismatchException.Type mismatchType = null;

        if (actualTotalVersions != expectedTotalVersions) {
            message += "total versions compared failed for key " + LogHelper.INSTANCE.hbaseKey(key);
            mismatchType = DataMismatchException.Type.TOTAL_VERSIONS_MISMATCH;
        } else {
            //Verifying snapshot load for all versions
            for (Map.Entry<Long, byte[]> instanceColumnEntry : columnsMap.get("instance").getVersionedValues().entrySet()) {
                if (!Data.INSTANCE.snapshotLoad.equals(new String(Bytes.toString(instanceColumnEntry.getValue())))) {
                    message += "payload compared failed for key " + LogHelper.INSTANCE.hbaseKey(key);
                    mismatchType = DataMismatchException.Type.PAYLOAD_MISMATCH;
                    break;
                }
            }
            //verifying if all versions are present
            for (Map.Entry<Long, byte[]> versionColumnEntry : columnsMap.get("version").getVersionedValues().entrySet()) {
                int actual = Integer.parseInt(Bytes.toString(versionColumnEntry.getValue()));
                Pair<DataMismatchException.Type, String> mismatch = checkVersionMismatch(actual, expectedVersion, key);
                message += mismatch.getSecond();
                mismatchType = mismatch.getFirst();
                expectedVersion--;
            }
        }

        if (mismatchType != null) {
            throw new DataMismatchException(mismatchType, message);
        }
    }

    private Pair<DataMismatchException.Type, String> checkVersionMismatch(int actual, Integer expectedVersion, String key) {
        String message = "";
        DataMismatchException.Type mismatchType = null;
        if (actual != expectedVersion) {
            if (actual > expectedVersion) {
                mismatchType = DataMismatchException.Type.READ_VERSION_HIGHER;
                message +=
                        "\nversion compared failed for key (read version higer) " + LogHelper.INSTANCE.hbaseKey(key) + " expected "
                                + expectedVersion + " actual " + actual;

            } else {
                mismatchType = DataMismatchException.Type.WRITE_VERSION_HIGHER;
                message +=
                        "\nversion compared failed for key (write version higer) " + LogHelper.INSTANCE.hbaseKey(key) + " expected "
                                + expectedVersion + " actual " + actual;
            }
        }
        return new Pair<>(mismatchType, message);
    }

    @Override
    public void shutDown() throws Exception {
        store.shutdown();
    }

    public KeyDistributor getKeyDistributor() {
        return keyDistributor;
    }
}
