package org.apache.bifromq.tenon.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.tenon.kafka.payload.SinkRecordPayload;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

final class KafkaPluginTest {
  @Test
  void sinkWaitsForEveryKafkaAcknowledgementAndPreservesRecordFields() throws Exception {
    var producer = producer();
    var plugin = plugin(producer);
    plugin.start();

    var first =
        SinkRecordPayload.newBuilder()
            .setTopic("output")
            .setPartition(2)
            .setTimestamp(1234)
            .setKey(ByteString.copyFromUtf8("key"))
            .setValue(ByteString.copyFromUtf8("first"))
            .addHeaders(
                SinkRecordPayload.KafkaHeader.newBuilder()
                    .setName("trace")
                    .setValue(ByteString.copyFromUtf8("one")))
            .build();
    var second = SinkRecordPayload.newBuilder().setTopic("output").build();

    var result =
        plugin.write(new FlowChannel("flow", 0), List.of(first, second)).toCompletableFuture();
    assertFalse(result.isDone());
    assertTrue(producer.completeNext());
    assertFalse(result.isDone());
    assertTrue(producer.completeNext());
    result.join();

    assertEquals(2, producer.history().size());
    var record = producer.history().getFirst();
    assertEquals("output", record.topic());
    assertEquals(2, record.partition());
    assertEquals(1234, record.timestamp());
    assertArrayEquals("key".getBytes(), record.key());
    assertArrayEquals("first".getBytes(), record.value());
    assertArrayEquals("one".getBytes(), record.headers().lastHeader("trace").value());
    assertNull(producer.history().get(1).key());
    assertNull(producer.history().get(1).value());
  }

  @Test
  void sinkFailureFailsTheBatch() throws Exception {
    var producer = producer();
    var plugin = plugin(producer);
    plugin.start();
    var payload =
        SinkRecordPayload.newBuilder()
            .setTopic("output")
            .setValue(ByteString.copyFromUtf8("value"))
            .build();

    var result = plugin.write(new FlowChannel("flow", 0), List.of(payload)).toCompletableFuture();
    producer.errorNext(new IllegalStateException("broker failed"));

    var error = assertThrows(java.util.concurrent.CompletionException.class, result::join);
    assertEquals("broker failed", error.getCause().getMessage());
  }

  @Test
  void completionsCommitOnlyTheContinuousSuccessfulPrefix() {
    var first = new CompletableFuture<AckCode>();
    var second = new CompletableFuture<AckCode>();
    var completions = new SourceCompletions();
    var firstPayload = sourcePayload("first", 10);
    var secondPayload = sourcePayload("second", 11);
    completions.add(firstPayload);
    completions.add(secondPayload);
    var sender =
        new PayloadSender<SourceRecordPayload>() {
          @Override
          public CompletableFuture<AckCode> send(int channel, SourceRecordPayload payload) {
            return payload.getOffset() == 10 ? first : second;
          }
        };
    completions.dispatch(1, sender);
    second.complete(AckCode.OK);
    completions.collectCompleted();
    assertTrue(completions.committableOffsets().isEmpty());
    first.complete(AckCode.OK);
    completions.collectCompleted();
    completions.dispatch(1, sender);
    second.complete(AckCode.OK);
    completions.collectCompleted();
    assertEquals(12, completions.committableOffsets().get(new TopicPartition("input", 0)).offset());
    completions.markCommitted(completions.committableOffsets());
    assertTrue(completions.committableOffsets().isEmpty());
  }

  @Test
  void completionsRetryTheSameRecordAfterBackpressure() {
    var attempts = new AtomicInteger();
    var completions = new SourceCompletions();
    var payload = sourcePayload("value", 4);
    completions.add(payload);
    PayloadSender<SourceRecordPayload> sender =
        (channel, ignored) -> {
          assertTrue(channel >= 0 && channel < 2);
          return CompletableFuture.completedFuture(
              attempts.incrementAndGet() == 1 ? AckCode.BACKPRESSURE : AckCode.OK);
        };
    completions.dispatch(2, sender);
    completions.collectCompleted();
    completions.dispatch(2, sender);
    completions.collectCompleted();
    assertEquals(2, attempts.get());
    assertEquals(5, completions.committableOffsets().get(new TopicPartition("input", 0)).offset());
  }

  @Test
  void completionsRejectPermanentTenonError() {
    var completions = new SourceCompletions();
    completions.add(sourcePayload("value", 0));
    completions.dispatch(1, (channel, payload) -> CompletableFuture.completedFuture(AckCode.ERROR));

    var error = assertThrows(IllegalStateException.class, completions::collectCompleted);
    assertEquals("Kafka Source record rejected by Tenon", error.getMessage());
  }

  @Test
  void producerCreationFailureDoesNotCreateConsumer() throws Exception {
    var plugin =
        new KafkaPlugin(
            config(),
            1,
            (channel, payload) -> CompletableFuture.completedFuture(AckCode.OK),
            new KafkaClientFactory() {
              @Override
              public Consumer<byte[], byte[]> createConsumer(java.util.Properties properties) {
                throw new AssertionError("Consumer must not be created by owner start");
              }

              @Override
              public Producer<byte[], byte[]> createProducer(java.util.Properties properties) {
                throw new IllegalStateException("producer failed");
              }
            });

    assertThrows(IllegalStateException.class, plugin::start);
    plugin.close();
  }

  @Test
  void retriesDoNotLetLaterOffsetsOvertakeAndAreBounded() {
    var completions = new SourceCompletions();
    completions.add(sourcePayload("first", 10));
    completions.add(sourcePayload("second", 11));
    var observed = new ArrayList<Long>();
    PayloadSender<SourceRecordPayload> retrying =
        (channel, payload) -> {
          observed.add(payload.getOffset());
          return CompletableFuture.completedFuture(AckCode.RETRY);
        };
    for (int attempt = 0; attempt < 3; attempt++) {
      completions.dispatch(1, retrying);
      completions.collectCompleted();
      assertTrue(completions.committableOffsets().isEmpty());
    }
    completions.dispatch(1, retrying);
    assertThrows(IllegalStateException.class, completions::collectCompleted);
    assertEquals(List.of(10L, 10L, 10L, 10L), observed);
  }

  @Test
  void backpressureWaitDoesNotExhaustRecordRetries() {
    var completions = new SourceCompletions();
    completions.add(sourcePayload("first", 10));
    for (int attempt = 0; attempt < 100; attempt++) {
      completions.dispatch(
          1, (channel, payload) -> CompletableFuture.completedFuture(AckCode.BACKPRESSURE));
      completions.collectCompleted();
    }
    assertTrue(completions.committableOffsets().isEmpty());
    completions.dispatch(1, (channel, payload) -> CompletableFuture.completedFuture(AckCode.OK));
    completions.collectCompleted();
    assertEquals(11, completions.committableOffsets().get(new TopicPartition("input", 0)).offset());
  }

  @Test
  void sinkPreservesDuplicateNullAndEmptyHeadersAndDefaultTopic() {
    var producer = producer();
    var payload =
        SinkRecordPayload.newBuilder()
            .setKey(ByteString.EMPTY)
            .setValue(ByteString.EMPTY)
            .addHeaders(SinkRecordPayload.KafkaHeader.newBuilder().setName("trace"))
            .addHeaders(
                SinkRecordPayload.KafkaHeader.newBuilder()
                    .setName("trace")
                    .setValue(ByteString.EMPTY))
            .build();
    var result = KafkaSink.write(producer, "fallback", List.of(payload)).toCompletableFuture();
    producer.completeNext();
    result.join();
    var record = producer.history().getFirst();
    assertEquals("fallback", record.topic());
    var headers = record.headers().toArray();
    assertEquals(2, headers.length);
    assertNull(headers[0].value());
    assertArrayEquals(new byte[0], headers[1].value());
    assertArrayEquals(new byte[0], record.key());
    assertArrayEquals(new byte[0], record.value());
    producer.close();
  }

  @Test
  void consumerPropertiesDisableAutomaticCommit() throws Exception {
    var config = config();

    assertEquals("false", config.consumerProperties().getProperty("enable.auto.commit"));
    assertEquals("earliest", config.consumerProperties().getProperty("auto.offset.reset"));
    assertEquals("all", config.producerProperties().getProperty("acks"));
    assertEquals("true", config.producerProperties().getProperty("enable.idempotence"));
  }

  private static KafkaPlugin plugin(MockProducer<byte[], byte[]> producer) throws Exception {
    return new KafkaPlugin(
        config(),
        1,
        (channel, payload) -> CompletableFuture.completedFuture(AckCode.OK),
        new KafkaClientFactory() {
          @Override
          public Consumer<byte[], byte[]> createConsumer(java.util.Properties properties) {
            return new MockConsumer<>("earliest");
          }

          @Override
          public Producer<byte[], byte[]> createProducer(java.util.Properties properties) {
            return producer;
          }
        });
  }

  private static KafkaConfig config() throws Exception {
    return KafkaConfig.parse(
        new ObjectMapper()
            .readTree(
                """
                {"bootstrapServers":"localhost:9092","groupId":"group","sourceTopics":["input"]}
                """));
  }

  private static MockProducer<byte[], byte[]> producer() {
    return new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static SourceRecordPayload sourcePayload(String value, long offset) {
    return SourceRecordPayload.newBuilder()
        .setTopic("input")
        .setPartition(0)
        .setOffset(offset)
        .setValue(ByteString.copyFromUtf8(value))
        .build();
  }
}
