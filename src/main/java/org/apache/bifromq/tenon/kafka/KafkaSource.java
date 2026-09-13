package org.apache.bifromq.tenon.kafka;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSource;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** Owns one Kafka Consumer and serializes all Kafka API calls on its worker thread. */
final class KafkaSource implements TenonSource {
  private enum Phase {
    RUNNING,
    QUIESCED,
    CLOSED
  }

  private final KafkaConfig config;
  private final int parallelism;
  private final int sourceIndex;
  private final PayloadSender<SourceRecordPayload> sender;
  private final KafkaClientFactory clients;
  // Accessed by the Consumer worker and its commit callbacks, which Kafka invokes from the
  // Consumer thread. Values suppress duplicate submissions of the same offset.
  private final Map<TopicPartition, Long> asyncSubmitted = new LinkedHashMap<>();
  private final Thread worker = Thread.ofPlatform().name("tenon-kafka-source").unstarted(this::run);
  private volatile Phase phase = Phase.RUNNING;
  private Consumer<byte[], byte[]> consumer;

  KafkaSource(
      KafkaConfig config,
      int parallelism,
      int sourceIndex,
      PayloadSender<SourceRecordPayload> sender,
      KafkaClientFactory clients) {
    this.config = config;
    this.parallelism = parallelism;
    this.sourceIndex = sourceIndex;
    this.sender = sender;
    this.clients = clients;
  }

  @Override
  public void start() {
    consumer = clients.createConsumer(config.consumerProperties(sourceIndex));
    worker.start();
  }

  @Override
  public void quiesce() {
    phase = Phase.QUIESCED;
    consumer.wakeup();
    LockSupport.unpark(worker);
  }

  @Override
  public void close() throws InterruptedException {
    phase = Phase.CLOSED;
    if (consumer == null) {
      return; // Consumer construction failed before ownership transferred.
    }
    if (worker.getState() == Thread.State.NEW) {
      consumer.close(CloseOptions.timeout(Duration.ofSeconds(10))); // No concurrent Consumer use.
      return;
    }
    consumer.wakeup();
    LockSupport.unpark(worker);
    worker.join();
  }

  private void run() {
    var completions = new SourceCompletions(() -> LockSupport.unpark(worker));
    try {
      consumer.subscribe(config.sourceTopics(), new Rebalance(completions));
      System.out.println("Kafka Source subscribed");
      while (phase != Phase.CLOSED) {
        try {
          if (phase == Phase.RUNNING) {
            var timeout =
                completions.hasPending()
                    ? Duration.ZERO
                    : Duration.ofMillis(config.pollTimeoutMs());
            for (var record : consumer.poll(timeout)) {
              completions.add(toSourcePayload(record));
            }
          } else {
            // Keep membership and late OK commits alive without fetching new business input.
            consumer.pause(consumer.assignment());
            consumer.poll(Duration.ZERO);
          }
          completions.collectCompleted();
          commitAsync(completions);
          if (phase == Phase.RUNNING) {
            completions.dispatch(parallelism, sender, () -> phase == Phase.RUNNING);
          }
          if (phase == Phase.RUNNING) {
            updatePause(completions);
          }
        } catch (WakeupException error) {
          if (phase == Phase.RUNNING) {
            throw error;
          }
        }
        if (phase != Phase.RUNNING || completions.hasPending()) {
          // Completion callbacks unpark immediately; retain the configured poll bound so Kafka
          // heartbeats continue while a downstream acknowledgement is outstanding.
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.max(1, config.pollTimeoutMs())));
        }
      }
      // SDK completes all admitted results before invoking close; never dispatch here.
      completions.collectCompleted();
      try {
        commitSync(completions);
      } catch (WakeupException shutdownWakeup) {
        // close may wake us between the final poll and commit. Retry this known wakeup only.
        commitSync(completions);
      }
      consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
      System.out.println("Kafka Source closed");
    } catch (Throwable error) {
      // Kafka exception messages can contain configuration values. Report only the error class.
      System.err.println("Kafka Source fatal: " + error.getClass().getName());
      System.err.flush();
      System.exit(1);
    }
  }

  private void commitAsync(SourceCompletions completions) {
    var offsets = completions.committableOffsets();
    var submit = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
    offsets.forEach(
        (partition, offset) -> {
          var submitted = asyncSubmitted.get(partition);
          if (submitted == null || offset.offset() > submitted) {
            submit.put(partition, offset);
            asyncSubmitted.put(partition, offset.offset());
          }
        });
    if (!submit.isEmpty()) {
      consumer.commitAsync(
          Map.copyOf(submit),
          (committed, error) -> {
            if (error != null) {
              submit.forEach(
                  (partition, offset) -> {
                    if (offset.offset() == asyncSubmitted.getOrDefault(partition, -1L)) {
                      asyncSubmitted.remove(partition);
                    }
                  });
              System.err.println("Kafka Source async commit failed: " + error.getClass().getName());
              System.err.flush();
              return;
            }
            submit.forEach(
                (partition, offset) -> {
                  if (offset.offset() == asyncSubmitted.getOrDefault(partition, -1L)) {
                    asyncSubmitted.remove(partition);
                  }
                });
            completions.markCommitted(submit);
          });
    }
  }

  private void commitSync(SourceCompletions completions) {
    var offsets = completions.committableOffsets();
    if (!offsets.isEmpty()) {
      consumer.commitSync(offsets);
      asyncSubmitted.clear();
      completions.markCommitted(offsets);
      System.out.println("Kafka Source committed completed records");
    }
  }

  private void updatePause(SourceCompletions completions) {
    var waiting = completions.partitionsWithPending();
    var assigned = consumer.assignment();
    consumer.pause(assigned.stream().filter(waiting::contains).toList());
    consumer.resume(assigned.stream().filter(partition -> !waiting.contains(partition)).toList());
  }

  private static SourceRecordPayload toSourcePayload(ConsumerRecord<byte[], byte[]> record) {
    var payload =
        SourceRecordPayload.newBuilder()
            .setTopic(record.topic())
            .setPartition(record.partition())
            .setOffset(record.offset())
            .setTimestamp(record.timestamp())
            .setTimestampType(record.timestampType().id);
    if (record.key() != null) {
      payload.setKey(ByteString.copyFrom(record.key()));
    }
    if (record.value() != null) {
      payload.setValue(ByteString.copyFrom(record.value()));
    }
    for (var header : record.headers()) {
      var mapped = SourceRecordPayload.KafkaHeader.newBuilder().setName(header.key());
      if (header.value() != null) {
        mapped.setValue(ByteString.copyFrom(header.value()));
      }
      payload.addHeaders(mapped);
    }
    return payload.build();
  }

  private final class Rebalance implements ConsumerRebalanceListener {
    private final SourceCompletions completions;

    private Rebalance(SourceCompletions completions) {
      this.completions = completions;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      completions.collectCompleted();
      var offsets = completions.committableOffsets(partitions);
      if (!offsets.isEmpty()) {
        consumer.commitSync(offsets);
        asyncSubmitted.keySet().removeAll(partitions);
        completions.markCommitted(offsets);
      }
      completions.remove(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      asyncSubmitted.keySet().removeAll(partitions);
      completions.remove(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      if (phase != Phase.RUNNING) {
        consumer.pause(partitions);
      }
    }
  }
}
