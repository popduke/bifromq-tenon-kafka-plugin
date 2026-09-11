package org.apache.bifromq.tenon.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

final class KafkaSourceTest {
  @Test
  void pollsOtherPartitionWhileFirstIsPendingAndCommitsLateOkAfterQuiesce() throws Exception {
    var consumer = new RecordingConsumer();
    var first = new TopicPartition("input", 0);
    var second = new TopicPartition("input", 1);
    var firstAck = new CompletableFuture<AckCode>();
    var secondSeen = new CompletableFuture<SourceRecordPayload>();
    var sent = new AtomicInteger();
    var source =
        source(
            consumer,
            (channel, payload) -> {
              sent.incrementAndGet();
              if (payload.getPartition() == 0) return firstAck;
              secondSeen.complete(payload);
              return CompletableFuture.completedFuture(AckCode.OK);
            });
    consumer.schedulePollTask(
        () -> {
          consumer.rebalance(List.of(first, second));
          consumer.updateBeginningOffsets(Map.of(first, 0L, second, 0L));
          consumer.addRecord(new ConsumerRecord<>("input", 0, 0, null, new byte[0]));
        });
    consumer.schedulePollTask(
        () -> {
          var record = new ConsumerRecord<byte[], byte[]>("input", 1, 0, null, new byte[0]);
          record.headers().add("nullable", null).add("empty", new byte[0]);
          consumer.addRecord(record);
        });
    source.start();
    try {
      var payload = secondSeen.get(5, TimeUnit.SECONDS);
      assertFalse(payload.hasKey());
      assertTrue(payload.hasValue());
      assertFalse(payload.getHeaders(0).hasValue());
      assertTrue(payload.getHeaders(1).hasValue());
      source.quiesce();
      assertFalse(consumer.closed());
      firstAck.complete(AckCode.OK);
    } finally {
      firstAck.complete(AckCode.OK);
      source.close();
    }
    assertTrue(consumer.closed());
    assertEquals(2, sent.get());
    assertEquals(1, consumer.finalOffsets.get(first).offset());
  }

  @Test
  void quiesceWaitsForSynchronousSendButNotItsCompletionAndPreventsNewAttempt() throws Exception {
    var consumer = new RecordingConsumer();
    var partition = new TopicPartition("input", 0);
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var pendingAck = new CompletableFuture<AckCode>();
    var attempts = new AtomicInteger();
    var source =
        source(
            consumer,
            (channel, payload) -> {
              attempts.incrementAndGet();
              entered.countDown();
              try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException error) {
                throw new IllegalStateException(error);
              }
              return pendingAck;
            });
    consumer.schedulePollTask(
        () -> {
          consumer.rebalance(List.of(partition));
          consumer.updateBeginningOffsets(Map.of(partition, 0L));
          consumer.addRecord(new ConsumerRecord<>("input", 0, 0, null, null));
          consumer.addRecord(new ConsumerRecord<>("input", 0, 1, null, null));
        });
    source.start();
    var quiesced = new CompletableFuture<Void>();
    var stopper =
        Thread.ofPlatform()
            .unstarted(
                () -> {
                  source.quiesce();
                  quiesced.complete(null);
                });
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      stopper.start();
      release.countDown();
      quiesced.get(5, TimeUnit.SECONDS);
      assertFalse(pendingAck.isDone());
      pendingAck.complete(AckCode.BACKPRESSURE);
    } finally {
      release.countDown();
      stopper.join();
      source.close();
    }
    assertEquals(1, attempts.get());
  }

  @Test
  void unstartedSourceOpensNoConsumerAndCloseReturns() throws Exception {
    var consumer = new RecordingConsumer();
    var source =
        source(consumer, (channel, payload) -> CompletableFuture.completedFuture(AckCode.OK));
    source.close();
    assertFalse(consumer.closed());
  }

  @Test
  void revokedPartitionCommitsCompletedPrefix() throws Exception {
    checkRebalance(RebalanceAction.REVOKE, 1);
  }

  @Test
  void lostPartitionDoesNotCommitEvenCompletedRecords() throws Exception {
    checkRebalance(RebalanceAction.LOST, 0);
  }

  private enum RebalanceAction {
    REVOKE,
    LOST
  }

  private static void checkRebalance(RebalanceAction action, int expectedCommits) throws Exception {
    var consumer = new RecordingConsumer();
    var partition = new TopicPartition("input", 0);
    var completed = new CountDownLatch(1);
    var source =
        source(consumer, (channel, payload) -> CompletableFuture.completedFuture(AckCode.OK));
    consumer.schedulePollTask(
        () -> {
          consumer.rebalance(List.of(partition));
          consumer.updateBeginningOffsets(Map.of(partition, 0L));
          consumer.addRecord(new ConsumerRecord<>("input", 0, 0, null, null));
        });
    consumer.schedulePollTask(
        () -> {
          if (action == RebalanceAction.REVOKE) {
            consumer.listener.onPartitionsRevoked(List.of(partition));
          } else {
            consumer.listener.onPartitionsLost(List.of(partition));
          }
          completed.countDown();
        });
    source.start();
    try {
      assertTrue(completed.await(5, TimeUnit.SECONDS));
    } finally {
      source.quiesce();
      source.close();
    }
    assertEquals(expectedCommits, consumer.commits.get());
  }

  private static final class RecordingConsumer extends MockConsumer<byte[], byte[]> {
    private Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> finalOffsets;
    private ConsumerRebalanceListener listener;
    private final AtomicInteger commits = new AtomicInteger();

    @Override
    public synchronized void subscribe(
        java.util.Collection<String> topics, ConsumerRebalanceListener callback) {
      listener = callback;
      super.subscribe(topics, callback);
    }

    @Override
    public synchronized void commitSync(
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
      commits.incrementAndGet();
      super.commitSync(offsets);
    }

    private RecordingConsumer() {
      super("earliest");
    }

    @Override
    public synchronized void close(CloseOptions options) {
      finalOffsets = committed(assignment());
      super.close(options);
    }
  }

  private static KafkaSource source(
      MockConsumer<byte[], byte[]> consumer, PayloadSender<SourceRecordPayload> sender)
      throws Exception {
    var config =
        KafkaConfig.parse(
            new ObjectMapper()
                .readTree(
                    """
        {"bootstrapServers":"localhost:9092","groupId":"test","sourceTopics":["input"],"pollTimeoutMs":10}
        """));
    return new KafkaSource(
        config,
        2,
        sender,
        new KafkaClientFactory() {
          @Override
          public Consumer<byte[], byte[]> createConsumer(Properties properties) {
            return consumer;
          }

          @Override
          public Producer<byte[], byte[]> createProducer(Properties properties) {
            throw new AssertionError("Unexpected producer creation");
          }
        });
  }
}
