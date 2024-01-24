package com.flipkart.yaktest.failtest.pulsar;

import com.flipkart.yaktest.Config;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.url.DataURLStreamHandler;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.shade.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PulsarConsumerFactory {

    private PulsarConsumerFactory() {
    }

    private static Logger logger = LoggerFactory.getLogger(PulsarConsumerFactory.class);
    private static final String SUBSCRIPTION_PREFIX = "YAK_TEST_";
    private static final Map<String, Consumer<byte[]>> topicToConsumerMap = new ConcurrentHashMap<>();
    private static PulsarClient pulsarClient;

    public static void init() throws PulsarClientException {
        shutdown();
        PulsarConfig pulsarConfig = Config.getInstance().getPulsarConfig();
        pulsarClient = createAuthenticatedClient(pulsarConfig);
        logger.info("creating pulsar client..");
        for (String topic : pulsarConfig.getTopics()) {
            String modifiedTopicName = topic.replace(':', '_').replace('/', '-');
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                    .subscriptionName(SUBSCRIPTION_PREFIX + modifiedTopicName)
                    .subscriptionType(SubscriptionType.Failover)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .batchReceivePolicy(BatchReceivePolicy.builder()
                            .maxNumBytes(512 * 1024).timeout(50, TimeUnit.MILLISECONDS).maxNumMessages(50).build())
                    .receiverQueueSize(1000)
                    .maxTotalReceiverQueueSizeAcrossPartitions(50000)
                    .acknowledgmentGroupTime(100, TimeUnit.MILLISECONDS)
                    .subscribe();
            topicToConsumerMap.putIfAbsent(topic, consumer);
        }
        logger.info("created pulsar clients for topics {}", topicToConsumerMap.keySet());
    }

    public static Consumer<byte[]> getConsumer(String topic) {
        if (topicToConsumerMap.containsKey(topic)) {
            return topicToConsumerMap.get(topic);
        }
        throw new IllegalArgumentException("Consumer for topic does not exists");
    }

    private static PulsarClient createAuthenticatedClient(PulsarConfig pulsarConfig) throws PulsarClientException {
        String pulsarEndpoint = pulsarConfig.getEndpoint();
        String issuerUrl = pulsarConfig.getAuthEndpoint();
        String clientId = pulsarConfig.getAuthClientId();
        String clientSecret = pulsarConfig.getAuthClientSecret();
        try {
            String audience = "authn";
            URL credentialsUrl = buildClientCredentials(clientId, clientSecret, issuerUrl);
            Authentication authn = AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl), credentialsUrl, audience);
            return PulsarClient.builder()
                    .serviceUrl(pulsarEndpoint)
                    .authentication(authn)
                    .statsInterval(5, TimeUnit.SECONDS)
                    .build();
        } catch (MalformedURLException ex) {
            logger.error("could not create client {}", pulsarConfig, ex);
            throw new PulsarClientException(ex);
        }
    }

    private static URL buildClientCredentials(String clientId, String clientSecret, String issuerUrl)
            throws MalformedURLException {
        Map<String, String> props = new HashMap<>();
        props.put("type", "client_credentials");
        props.put("client_id", clientId);
        props.put("client_secret", clientSecret);
        props.put("issuer_url", issuerUrl);
        Gson gson = new Gson();
        String json = gson.toJson(props);
        String encoded = Base64.getEncoder().encodeToString(json.getBytes());
        String data = "data:application/json;base64," + encoded;
        return new URL(null, data, new DataURLStreamHandler());
    }

    public static void shutdown() {
        for (Map.Entry<String, Consumer<byte[]>> consumerEntry : topicToConsumerMap.entrySet()) {
            try {
                consumerEntry.getValue().close();
            } catch (PulsarClientException e) {
                logger.error("could not close consumer for topic {}", consumerEntry.getKey(), e);
            }
        }
        boolean allDisconnected = true;
        for (Map.Entry<String, Consumer<byte[]>> consumerEntry : topicToConsumerMap.entrySet()) {
            if (consumerEntry.getValue().isConnected()) {
                allDisconnected = false;
            }
        }
        if (allDisconnected) {
            logger.info("all consumers are closed");
            topicToConsumerMap.clear();
        }
        if (pulsarClient != null) {
            try {
                pulsarClient.shutdown();
            } catch (PulsarClientException e) {
                logger.error("could not close pulsar client for topic", e);
            }
        }
    }
}
