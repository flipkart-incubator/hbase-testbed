package com.flipkart.yaktest.interruption.models;

import com.flipkart.yaktest.Config;

public class SSHConfig {

    private String user;
    private String passphrase;
    private String sshKeyPath;

    private static SSHConfig sshConfig;

    private SSHConfig() {

    }

    private static void buildSSHConfig() {
        sshConfig = new SSHConfig();
        sshConfig.setUser(Config.getInstance().getUsername());
        sshConfig.setPassphrase(Config.getInstance().getPassphrase());
        sshConfig.setSshKeyPath(Config.getInstance().getSshKeyPath());
    }

    public synchronized static SSHConfig getInstance() {
        if (sshConfig == null) {
            buildSSHConfig();
        }
        return sshConfig;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
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
}
