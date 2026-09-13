package org.apache.bifromq.tenon.kafka;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.kafka.payload.SinkRecordPayload;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSource;
import org.apache.bifromq.tenon.sdk.TenonSourceAndSink;
import org.apache.kafka.clients.producer.Producer;

/** Owns the Producer; each created Source owns its Consumer and thread. */
final class KafkaPlugin implements TenonSourceAndSink<SinkRecordPayload> {
  private final KafkaConfig config;
  private final int parallelism;
  private final PayloadSender<SourceRecordPayload> sender;
  private final KafkaClientFactory clients;
  private int nextSourceIndex;
  private Producer<byte[], byte[]> producer;

  KafkaPlugin(
      KafkaConfig config,
      int parallelism,
      PayloadSender<SourceRecordPayload> sender,
      KafkaClientFactory clients) {
    this.config = config;
    this.parallelism = parallelism;
    this.sender = sender;
    this.clients = clients;
  }

  @Override
  public void start() {
    producer = clients.createProducer(config.producerProperties());
  }

  @Override
  public TenonSource createSource() {
    return new KafkaSource(config, parallelism, nextSourceIndex++, sender, clients);
  }

  @Override
  public CompletionStage<Void> write(FlowChannel channel, List<SinkRecordPayload> records) {
    return KafkaSink.write(producer, config.defaultTopic(), records);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close(Duration.ofSeconds(10));
    }
  }
}
