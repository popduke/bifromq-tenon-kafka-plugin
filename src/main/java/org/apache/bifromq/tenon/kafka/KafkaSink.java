package org.apache.bifromq.tenon.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.kafka.payload.SinkRecordPayload;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

final class KafkaSink {
  private KafkaSink() {}

  static CompletionStage<Void> write(
      Producer<byte[], byte[]> producer, String defaultTopic, List<SinkRecordPayload> records) {
    var results = new ArrayList<CompletableFuture<Void>>(records.size());
    for (var payload : records) {
      var topic = payload.getTopic().isEmpty() ? defaultTopic : payload.getTopic();
      if (topic.isEmpty()) {
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("Kafka sink topic is required"));
      }
      var result = new CompletableFuture<Void>();
      results.add(result);
      try {
        producer.send(
            toProducerRecord(topic, payload),
            (metadata, error) -> {
              if (error == null) {
                result.complete(null);
              } else {
                result.completeExceptionally(error);
              }
            });
      } catch (RuntimeException error) {
        result.completeExceptionally(error);
      }
    }
    return CompletableFuture.allOf(results.toArray(CompletableFuture[]::new))
        .whenComplete(
            (ignored, error) -> {
              if (error == null) {
                System.out.println("Kafka Sink delivered a batch");
              }
            });
  }

  private static ProducerRecord<byte[], byte[]> toProducerRecord(
      String topic, SinkRecordPayload payload) {
    var record =
        new ProducerRecord<>(
            topic,
            payload.hasPartition() ? payload.getPartition() : null,
            payload.hasTimestamp() ? payload.getTimestamp() : null,
            payload.hasKey() ? payload.getKey().toByteArray() : null,
            payload.hasValue() ? payload.getValue().toByteArray() : null);
    for (var header : payload.getHeadersList()) {
      record
          .headers()
          .add(
              new RecordHeader(
                  header.getName(), header.hasValue() ? header.getValue().toByteArray() : null));
    }
    return record;
  }
}
