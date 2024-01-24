package com.flipkart.yaktest.interruption.utils;

import com.flipkart.yaktest.interruption.exception.ShellCommandException;
import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.flipkart.yaktest.interruption.models.ShellExitStatus.FAIL;

public class ShellUtils {

    private static Logger logger = LoggerFactory.getLogger(ShellUtils.class);

    public static String runShellCommand(String host, String user, String passPhrase, String sshKeyPath, String command) throws ShellCommandException {

        Channel channel = null;
        Session session = null;
        StringBuilder output = new StringBuilder("");

        try {
            session = buildSession(host, user, passPhrase, sshKeyPath);
            logger.info("connecting to  {} as user {} using shhKeyPath {}", host, user, sshKeyPath);
            session.connect();
            logger.info("Connected to remote host \"{}\" ", host);

            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            channel.connect();
            byte[] tmp = new byte[1024];

            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    output.append(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (channel.getExitStatus() != 0) {
                        logger.error("remote host exit status: " + channel.getExitStatus() + " host: " + host + " command: " + command);
                        throw new ShellCommandException(FAIL);
                    }
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    //pass e
                }
            }
        } catch (Exception e) {
            throw new ShellCommandException(FAIL, e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }

        logger.info("command \"{}\" ran successfully with output => \"{}\" ", command, output.toString());
        return output.toString();
    }

    private static Session buildSession(String host, String user, String passphrase, String sshKeyPath) throws JSchException {

        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");

        JSch jsch = new JSch();

        jsch.addIdentity(sshKeyPath, null, passphrase.getBytes());

        Session session = jsch.getSession(user, host, 22);
        session.setConfig(config);
        return session;

    }
}
