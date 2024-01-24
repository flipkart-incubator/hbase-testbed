package com.flipkart.yaktest;

import com.flipkart.yaktest.interruption.models.YakComponent;

import java.util.Map;

public class ShellCommands {

    public Map<YakComponent, String> start;
    public Map<YakComponent, String> stop;
    public Map<YakComponent, String> kill;
    public Map<YakComponent, String> status;

    public String regionServerReloadCommand;
    public String masterInconsistenciesCommand;
    public String masterShellCommand;
    public String hbaseCommand;
    public String hadoopCommand;
    public String validateRegionRackAwarenessCommand;
}
