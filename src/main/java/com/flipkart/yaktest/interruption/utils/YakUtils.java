package com.flipkart.yaktest.interruption.utils;

import com.flipkart.yaktest.Config;
import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.flipkart.yaktest.interruption.models.SSHConfig;
import com.flipkart.yaktest.interruption.models.YakComponent;
import com.flipkart.yaktest.models.RegionInfo;
import com.flipkart.yaktest.output.ProcessStatus;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YakUtils {

    private YakUtils() {
    }

    private static final String COMMAND_REPLACE_STRING = "\\{\\}";
    private static final String CREATE_NETWORK_PARTITION_COMMAND =
            "sudo iptables -A INPUT -p tcp --dport ssh -j ACCEPT; sudo iptables -A OUTPUT -p tcp --sport 22 -j ACCEPT; sudo iptables -A OUTPUT -p tcp  -j DROP; sudo iptables -A INPUT -p tcp  -j DROP;";
    private static final String REMOVE_NETWORK_PARTITION_COMMAND = "sudo iptables -F";
    private static final String REPLICATION_DISABLE_COMMAND =
            "echo \"alter '%s:%s',{NAME=>'state',REPLICATION_SCOPE => '0'} \" | {}";
    private static final String REPLICATION_ENABLE_COMMAND =
            "echo \"alter '%s:%s',{NAME=>'state',REPLICATION_SCOPE => '1'} \" | {}";
    private static final String ALL_CF_REPLICATION_ENABLE_COMMAND =
            "echo \"alter '%s:%s',{NAME=>'state',REPLICATION_SCOPE => '1'}, {NAME=>'data',REPLICATION_SCOPE => '1'} \" | {}";
    private static final String GET_REGION_COMMAND =
            "echo \"scan 'hbase:meta'\" | {} | grep %s:%s | grep \"info:regioninfo\" | grep -v \"OFFLINE => true\" | "
                    + "awk '{ print $1}'";
    private static final String SPLIT_REGION_COMMAND = "echo \"split '{}' \" | {}";
    private static final String MERGE_REGION_COMMAND = "echo \"merge_region '{}', '{}', true \" | {}";
    private static final String KAFKA_CONNECTION_CHECK_COMMAND = "netstat | grep 9092 | grep ESTABLISHED | grep kafka | awk '{ print $5 }'";
    private static final String CREATE_NAMESPACE = "echo \"create_namespace '%s', 'hbase.rsgroup.name' => '%s'\" | {}";
    private static final String DISABLE_DROP_COMMAND = "echo \"disable '%s:%s'; drop '%s:%s'\" | {}";
    private static final String DROP_CREATE_TABLE_COMMAND =
            "echo \"disable '%s:%s'; drop '%s:%s'; sleep 30; create '%s:%s',{NAME=>'data',TTL=>'7200',COMPRESSION=>'GZ', REPLICATION_SCOPE => '1', VERSIONS => 5}, "
                    + "{NAME=>'state',TTL=>'7200',COMPRESSION=>'GZ', REPLICATION_SCOPE => '%s', VERSIONS => '%s'}, {SPLITS=>['20','40','50','60','70','80']}, {CONFIGURATION =>"
                    + " {'hbase.hregion.max.filesize' => '268435456'}}\" | {}";
    private static final String ENCODED_REGION_COMMAND =
            "echo \"scan 'hbase:meta'\" | {} | grep \"info:regioninfo\" | grep \"%s:%s\" | awk '{ print $1}' | " + "cut -d '"
                    + ".' -f2";
    private static final String ASSIGN_REGION_COMMAND = "echo \"assign '{}'\" | {}";
    private static final String REGIONS_INFO_COMMAND =
            "echo \"scan 'hbase:meta'\" | {} | grep \"%s:%s\" | grep \"info:regioninfo\"  |  awk '{ print $1,$12 $15}'"
                    + "  | cut -d '}' -f1";
    private static final String OPEN_STATE_REGIONS_COMMAND =
            "echo \"scan 'hbase:meta'\" | {} | grep \"%s:%s\" | grep \"info:state\"  | grep \"OPEN\" | awk '{ print $1}'";
    private static final String GET_ACTIVE_NN_COMMAND =
            "if [[ $({} haadmin -getServiceState nn1) == \"active\" ]]; then echo \"$({} getconf -nnRpcAddresses | grep 'nn-1')\"; else echo \"$({} getconf -nnRpcAddresses | grep 'nn-2')\"; fi";
    private static final String CREATE_BACKUP_SET_COMMAND = "{} backup set add yaktest {}";
    private static final String BACKUP_ID_COMMAND =
            "{} backup history 2> /dev/null | grep \"ID=backup_\" | head -1 | grep \"State=COMPLETE\" | sed 's/,.*//' | sed 's/.*=//'";
    private static final String CREATE_BACKUP_COMMAND =
            "{} backup create full hdfs:///tmp/backup -s yaktest";
    private static final String CREATE_RESTORE_COMMAND =
            "{} restore hdfs:///tmp/backup {} -t {} -m {} -o";

    public static String getTableNameWithNamespace(String namespace, String tableName) {
        return namespace + ":" + tableName;
    }

    public static String startComponent(String host, YakComponent yakComponent, SSHConfig sshConfig) throws ShellCommandException {
        String command = Config.getInstance().getShellCommands().start.get(yakComponent);
        return ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
    }

    public static void startComponents(List<String> hosts, YakComponent yakComponent, SSHConfig sshConfig) throws ShellCommandException {
        String command = Config.getInstance().getShellCommands().start.get(yakComponent);
        for (String host : hosts) {
            ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        }
    }

    public static void stopComponents(List<String> hosts, YakComponent yakComponent, SSHConfig sshConfig) throws ShellCommandException {
        String command = Config.getInstance().getShellCommands().stop.get(yakComponent);
        for (String host : hosts) {
            ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        }
    }

    public static ProcessStatus isProcessRunning(String host, YakComponent yakComponent, SSHConfig sshConfig) throws ShellCommandException {
        String command = Config.getInstance().getShellCommands().status.get(yakComponent);
        String output = ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        try {
            Integer.parseInt(output.replaceAll("[^0-9]+", ""));
            return new ProcessStatus(host, true);
        } catch (Exception e) {
            return new ProcessStatus(host, false);
        }
    }

    public static void killComponents(List<String> hosts, YakComponent yakComponent, SSHConfig sshConfig) throws ShellCommandException {
        String command = Config.getInstance().getShellCommands().kill.get(yakComponent);
        for (String host : hosts) {
            ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        }
    }

    public static void createPartition(List<String> hosts, SSHConfig sshConfig) throws ShellCommandException {

        String command = CREATE_NETWORK_PARTITION_COMMAND;
        for (String host : hosts) {
            ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        }
    }

    public static void removePartition(List<String> hosts, SSHConfig sshConfig) throws ShellCommandException {
        String command = REMOVE_NETWORK_PARTITION_COMMAND;
        for (String host : hosts) {
            ShellUtils.runShellCommand(host, sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
        }
    }

    public static String checkInconsistencies(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(Config.getInstance().getShellCommands().masterInconsistenciesCommand, YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()));
        return ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static String validateRegionRackAwareness(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(Config.getInstance().getShellCommands().validateRegionRackAwarenessCommand, YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()));
        return ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static String disableKafkaReplication(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(REPLICATION_DISABLE_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        return ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static String enableKafkaReplication(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(REPLICATION_ENABLE_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        return ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static String enableClusterReplication(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(ALL_CF_REPLICATION_ENABLE_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        return ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static List<String> getRegions(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(GET_REGION_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        String output = ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
        return Arrays.asList(output.split("\n"));
    }

    public static void splitRegion(String region, SSHConfig sshConfig) throws ShellCommandException {
        String command = SPLIT_REGION_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, region).replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().masterShellCommand);
        ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static void mergeRegions(String region1, String region2, SSHConfig sshConfig) throws ShellCommandException {
        String command = MERGE_REGION_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, region1).replaceFirst(COMMAND_REPLACE_STRING, region2).replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().masterShellCommand);
        ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static void createNamespace(SSHConfig sshConfig, String site, String rsgroup) throws ShellCommandException {
        String command = String.format(CREATE_NAMESPACE.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), rsgroup);
        ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig(site).get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static void disableDropBackupTable(SSHConfig sshConfig, String site) throws ShellCommandException {
        String command = String
                .format(DISABLE_DROP_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand),
                        Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup(), Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup());
        ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig(site).get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
    }

    public static void dropAndCreateTable(SSHConfig sshConfig, boolean isReplicationEnabled, String site) throws ShellCommandException {
        String command = String
                .format(DROP_CREATE_TABLE_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName(), Config.getInstance().getNamespace(), Config.getInstance().getTableName(),
                        Config.getInstance().getNamespace(), Config.getInstance().getTableName(), isReplicationEnabled ? "1" : "0", Config.getInstance().getTotalVersions());
        ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig(site).get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
    }

    public static List<String> fetchEncodedRegions(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(ENCODED_REGION_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        String output = ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);
        return Arrays.asList(output.split("\n"));
    }

    public static String getActiveNN(SSHConfig sshConfig) throws ShellCommandException {
        String command = GET_ACTIVE_NN_COMMAND.replaceAll(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().hadoopCommand);
        return ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command).trim();
    }

    public static String getlastBackupId(SSHConfig sshConfig) throws ShellCommandException {
        String command = BACKUP_ID_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().hbaseCommand);
        return ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command).trim();
    }

    public static void createBackupSet(SSHConfig sshConfig) throws ShellCommandException {
        String command = CREATE_BACKUP_SET_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().hbaseCommand)
                .replaceFirst(COMMAND_REPLACE_STRING, getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()));
        ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
    }

    public static void performBackup(SSHConfig sshConfig) throws ShellCommandException {
        String command = CREATE_BACKUP_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().hbaseCommand);
        ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
    }

    public static void performRestore(SSHConfig sshConfig) throws ShellCommandException {
        String command = CREATE_RESTORE_COMMAND.replaceFirst(COMMAND_REPLACE_STRING, Config.getInstance().getShellCommands().hbaseCommand)
                .replaceFirst(COMMAND_REPLACE_STRING, getlastBackupId(sshConfig))
                .replaceFirst(COMMAND_REPLACE_STRING, getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableName()))
                .replaceFirst(COMMAND_REPLACE_STRING, YakUtils.getTableNameWithNamespace(Config.getInstance().getNamespace(), Config.getInstance().getTableNameBackup()));
        ShellUtils.runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0),
                sshConfig.getUser(), sshConfig.getPassphrase(), sshConfig.getSshKeyPath(), command);
    }

    public static Map<String, RegionInfo> fetchRegionsInfo(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(REGIONS_INFO_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        String output = ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);

        String[] regionsInfo = output.split("\n");
        return Arrays.asList(regionsInfo).stream().map(rInfo -> {
            String[] rInfoSplits = rInfo.split(" ");
            String regionName = rInfoSplits[0];
            String[] startEndKeySplits = rInfoSplits[1].split(",");
            return new RegionInfo(regionName, startEndKeySplits[0], startEndKeySplits[1]);
        }).collect(Collectors.toMap(RegionInfo::getRegionName, value -> value));
    }

    public static List<String> fetchOpenStateRegions(SSHConfig sshConfig) throws ShellCommandException {
        String command = String.format(OPEN_STATE_REGIONS_COMMAND.replace("{}", Config.getInstance().getShellCommands().masterShellCommand), Config.getInstance().getNamespace(), Config.getInstance().getTableName());
        String output = ShellUtils
                .runShellCommand(Config.getInstance().getYakComponentConfig().get(YakComponent.MASTER).get(0), sshConfig.getUser(), sshConfig.getPassphrase(),
                        sshConfig.getSshKeyPath(), command);

        String[] regions = output.split("\n");
        return Arrays.asList(regions);
    }
}
