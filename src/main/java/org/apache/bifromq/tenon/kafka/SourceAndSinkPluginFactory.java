package org.apache.bifromq.tenon.kafka;

import org.apache.bifromq.tenon.kafka.payload.SinkRecordPayload;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSourceAndSink;
import org.apache.bifromq.tenon.sdk.TenonSourceAndSinkFactory;
import tools.jackson.databind.JsonNode;

/** Creates the Kafka business owner without opening external resources. */
public final class SourceAndSinkPluginFactory
    implements TenonSourceAndSinkFactory<SourceRecordPayload, SinkRecordPayload> {
  @Override
  public TenonSourceAndSink<SinkRecordPayload> create(
      JsonNode config, int parallelism, PayloadSender<SourceRecordPayload> sender) {
    return new KafkaPlugin(
        KafkaConfig.parse(config), parallelism, sender, KafkaClientFactory.official());
  }
}
