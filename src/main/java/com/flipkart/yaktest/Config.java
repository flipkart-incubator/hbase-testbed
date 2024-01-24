package com.flipkart.yaktest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.yak.client.pipelined.config.MultiZoneStoreConfig;
import com.flipkart.yaktest.failtest.pulsar.PulsarConfig;
import com.flipkart.yaktest.interruption.models.KafkaConfigKey;
import com.flipkart.yaktest.interruption.models.YakComponent;
import joptsimple.internal.Strings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    private static Config config;
    private MultiZoneStoreConfig multiZoneStoreConfig;
    private Map<String, Map<YakComponent, List<String>>> componentConfigs;
    private Map<KafkaConfigKey, List<String>> kafkaConfig;
    private PulsarConfig pulsarConfig;
    private String defaultSite;
    private String username;
    private String passphrase;
    private String sshKeyPath;
    private String yakConsoleEndpoint;
    private String rsgroup;
    private String replicaRsgroup;
    private String namespace;
    private String tableName;

    private String totalVersions;
    private String tableNameBackup;
    private String userencode;
    private long replicationBuffer;
    private boolean checkRackAwarness = false;

    public boolean isCheckRackAwarness() {
        return checkRackAwarness;
    }

    public void setCheckRackAwarness(boolean checkRackAwarness) {
        this.checkRackAwarness = checkRackAwarness;
    }

    public void setReplicationBuffer(long replicationBuffer) {
        this.replicationBuffer = replicationBuffer;
    }

    public long getReplicationBuffer() {
        return replicationBuffer;
    }

    private static String customConfigPath;
    private ShellCommands shellCommands;

    private Config() {

    }

    private static void loadConfig() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            if (Strings.isNullOrEmpty(customConfigPath)) {
                config = mapper.readValue(MainClass.class.getResourceAsStream("/config.json"), Config.class);
            } else {
                config = mapper.readValue(new FileInputStream(new File(customConfigPath)), Config.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static Config getInstance() {
        if (config == null) {
            loadConfig();
        }
        return config;
    }

    public PulsarConfig getPulsarConfig() {
        return pulsarConfig;
    }

    public void setPulsarConfig(PulsarConfig pulsarConfig) {
        this.pulsarConfig = pulsarConfig;
    }

    public MultiZoneStoreConfig getMultiZoneStoreConfig() {
        return multiZoneStoreConfig;
    }

    public void setMultiZoneStoreConfig(MultiZoneStoreConfig multiZoneStoreConfig) {
        this.multiZoneStoreConfig = multiZoneStoreConfig;
    }

    public void setComponentConfigs(Map<String, Map<YakComponent, List<String>>> componentConfigs) {
        this.componentConfigs = componentConfigs;
    }

    public Map<String, Map<YakComponent, List<String>>> getComponentConfigs() {
        return this.componentConfigs;
    }

    public Map<YakComponent, List<String>> getYakComponentConfig() {
        return this.componentConfigs.get(defaultSite);
    }

    public Map<YakComponent, List<String>> getYakComponentConfig(String site) {
        if (this.componentConfigs.containsKey(site)) {
            return this.componentConfigs.get(site);
        }
        return this.componentConfigs.get(defaultSite);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassphrase() {
        return passphrase;
    }

    public void setPassphrase(String passphrase) {
        this.passphrase = passphrase;
    }

    public String getSshKeyPath() {
        return sshKeyPath;
    }

    public void setSshKeyPath(String sshKeyPath) {
        this.sshKeyPath = sshKeyPath;
    }

    public String getYakConsoleEndpoint() {
        return yakConsoleEndpoint;
    }

    public void setYakConsoleEndpoint(String yakConsoleEndpoint) {
        this.yakConsoleEndpoint = yakConsoleEndpoint;
    }

    public String getDefaultSite() {
        return this.defaultSite;
    }

    public void setDefaultSite(String defaultSite) {
        this.defaultSite = defaultSite;
    }

    public String getRsgroup() {
        return rsgroup;
    }

    public void setRsgroup(String rsgroup) {
        this.rsgroup = rsgroup;
    }

    public String getReplicaRsgroup() {
        return replicaRsgroup;
    }

    public void setReplicaRsgroup(String replicaRsgroup) {
        this.replicaRsgroup = replicaRsgroup;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTotalVersions() {
        return totalVersions;
    }

    public void setTotalVersions(String totalVersions) {
        this.totalVersions = totalVersions;
    }

    public String getTableNameBackup() {
        return tableNameBackup;
    }

    public void setTableNameBackup(String tableNameBackup) {
        this.tableNameBackup = tableNameBackup;
    }

    public String getUserencode() {
        return userencode;
    }

    public void setUserencode(String userencode) {
        this.userencode = userencode;
    }

    public ShellCommands getShellCommands() {
        return shellCommands;
    }

    public void setShellCommands(ShellCommands shellCommands) {
        this.shellCommands = shellCommands;
    }

    public static void setCustomConfigPath(String customConfigPath) {
        Config.customConfigPath = customConfigPath;
    }

    public Map<KafkaConfigKey, List<String>> getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(Map<String, List<String>> kafkaConfig) {
        Map<KafkaConfigKey, List<String>> kafkaConfigMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : kafkaConfig.entrySet()) {
            kafkaConfigMap.put(KafkaConfigKey.getByValue(entry.getKey()), entry.getValue());
        }
        this.kafkaConfig = kafkaConfigMap;
    }
}