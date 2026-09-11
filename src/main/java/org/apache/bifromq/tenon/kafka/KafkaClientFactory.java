package org.apache.bifromq.tenon.kafka;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

interface KafkaClientFactory {
  Consumer<byte[], byte[]> createConsumer(Properties properties);

  Producer<byte[], byte[]> createProducer(Properties properties);

  static KafkaClientFactory official() {
    return new KafkaClientFactory() {
      @Override
      public Consumer<byte[], byte[]> createConsumer(Properties properties) {
        return new KafkaConsumer<>(properties);
      }

      @Override
      public Producer<byte[], byte[]> createProducer(Properties properties) {
        return new KafkaProducer<>(properties);
      }
    };
  }
}
