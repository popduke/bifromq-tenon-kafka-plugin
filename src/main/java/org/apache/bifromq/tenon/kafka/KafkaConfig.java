package org.apache.bifromq.tenon.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import tools.jackson.databind.JsonNode;

record KafkaConfig(
    String bootstrapServers,
    String groupId,
    List<String> sourceTopics,
    int sourceConsumerCount,
    String defaultTopic,
    int pollTimeoutMs,
    int maxPollRecords,
    String autoOffsetReset,
    String clientId,
    String securityProtocol,
    String saslMechanism,
    String saslJaasConfig,
    String sslTruststoreLocation,
    String sslTruststorePassword,
    String sslKeystoreLocation,
    String sslKeystorePassword,
    String sslKeyPassword,
    String compressionType,
    int lingerMs,
    int batchSize,
    int deliveryTimeoutMs) {
  static KafkaConfig parse(JsonNode config) {
    var topics = new ArrayList<String>();
    for (var topic : config.required("sourceTopics")) {
      topics.add(topic.stringValue());
    }
    return new KafkaConfig(
        config.required("bootstrapServers").stringValue(),
        config.required("groupId").stringValue(),
        List.copyOf(topics),
        integer(config, "sourceConsumerCount", 1),
        text(config, "defaultTopic", ""),
        integer(config, "pollTimeoutMs", 1000),
        integer(config, "maxPollRecords", 500),
        text(config, "autoOffsetReset", "earliest"),
        text(config, "clientId", ""),
        text(config, "securityProtocol", "PLAINTEXT"),
        text(config, "saslMechanism", ""),
        text(config, "saslJaasConfig", ""),
        text(config, "sslTruststoreLocation", ""),
        text(config, "sslTruststorePassword", ""),
        text(config, "sslKeystoreLocation", ""),
        text(config, "sslKeystorePassword", ""),
        text(config, "sslKeyPassword", ""),
        text(config, "compressionType", "none"),
        integer(config, "lingerMs", 5),
        integer(config, "batchSize", 16384),
        integer(config, "deliveryTimeoutMs", 120000));
  }

  Properties consumerProperties(int sourceIndex) {
    var properties = baseProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
    if (!clientId.isEmpty()) {
      properties.put("client.id", clientId + "-source-" + sourceIndex);
    }
    return properties;
  }

  Properties producerProperties() {
    var properties = baseProperties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(lingerMs));
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(batchSize));
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(deliveryTimeoutMs));
    return properties;
  }

  private Properties baseProperties() {
    var properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("security.protocol", securityProtocol);
    putIfPresent(properties, "client.id", clientId);
    putIfPresent(properties, "sasl.mechanism", saslMechanism);
    putIfPresent(properties, "sasl.jaas.config", saslJaasConfig);
    putIfPresent(properties, "ssl.truststore.location", sslTruststoreLocation);
    putIfPresent(properties, "ssl.truststore.password", sslTruststorePassword);
    putIfPresent(properties, "ssl.keystore.location", sslKeystoreLocation);
    putIfPresent(properties, "ssl.keystore.password", sslKeystorePassword);
    putIfPresent(properties, "ssl.key.password", sslKeyPassword);
    return properties;
  }

  private static void putIfPresent(Properties properties, String name, String value) {
    if (!value.isEmpty()) {
      properties.put(name, value);
    }
  }

  private static String text(JsonNode config, String name, String fallback) {
    return config.has(name) ? config.required(name).stringValue() : fallback;
  }

  private static int integer(JsonNode config, String name, int fallback) {
    return config.has(name) ? config.required(name).intValue() : fallback;
  }
}
