package com.flipkart.yaktest.interruption.utils;

import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.jcraft.jsch.agentproxy.*;
import com.jcraft.jsch.agentproxy.sshj.AuthAgent;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.method.AuthMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ShellUtilsWithAgent {

    private static Logger logger = LoggerFactory.getLogger(ShellUtils.class);

    private final static Config config = new DefaultConfig();

    public static String runShellCommand(String host, String user, String passPhrase, String sshKeyPath, String command) throws ShellCommandException {
        String output = new String();
        SSHClient ssh = new SSHClient(config);
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        AgentProxy agentProxy = getAgentProxy();

        if (agentProxy == null) {
            logger.error("Could not find or connect to an agent");
        }

        try {
            ssh.loadKnownHosts();
            ssh.connect(host);
            ssh.auth(user, getAuthMethods(agentProxy));
            Session session = ssh.startSession();
            logger.info("Started session to remote host \"{}\" ", host);

            Session.Command cmd = session.exec(command);
            InputStream inputStream = cmd.getInputStream();
            output = IOUtils.readFully(cmd.getInputStream()).toString();
        } catch (IOException ex) {
            logger.info("Error while executing command => " + command + " as user => " + user + " on host => " + host, ex);
        } catch (Exception ex) {
            logger.info("Error while executing command => " + command + " as user => " + user + " on host => " + host, ex);
        } finally {
            try {
                ssh.disconnect();
            } catch (IOException ex) {
                logger.info("Failed to disconnect the ssh client session", ex);
            }
        }
        logger.info("command \"{}\" ran successfully with output => \"{}\" ", command, output.toString());
        return output.toString();
    }

    private static AgentProxy getAgentProxy() {
        Connector connector = getAgentConnector();
        if (connector != null) return new AgentProxy(connector);
        return null;
    }

    private static Connector getAgentConnector() {
        try {
            return ConnectorFactory.getDefault().createConnector();
        } catch (AgentProxyException ex) {
            logger.error("Failed to get AgentProxy", ex);
        }
        return null;
    }

    private static List<AuthMethod> getAuthMethods(AgentProxy agent) throws Exception {
        Identity[] identities = agent.getIdentities();
        List<AuthMethod> result = new ArrayList<AuthMethod>();
        for (Identity identity : identities) {
            result.add(new AuthAgent(agent, identity));
        }
        return result;
    }
}
