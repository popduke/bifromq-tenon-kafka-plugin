package org.apache.bifromq.tenon.kafka;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSource;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** Serializes Consumer access while quiesce fences synchronous send calls only. */
final class KafkaSource implements TenonSource {
  private enum Phase {
    RUNNING,
    QUIESCED,
    CLOSED
  }

  private final KafkaConfig config;
  private final int parallelism;
  private final PayloadSender<SourceRecordPayload> sender;
  private final KafkaClientFactory clients;
  private final Object admission = new Object();
  private final Thread worker = Thread.ofPlatform().name("tenon-kafka-source").unstarted(this::run);
  private volatile Phase phase = Phase.RUNNING;
  private Consumer<byte[], byte[]> consumer;

  KafkaSource(
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
    consumer = clients.createConsumer(config.consumerProperties());
    worker.start();
  }

  @Override
  public void quiesce() {
    synchronized (admission) {
      phase = Phase.QUIESCED;
    }
    consumer.wakeup();
    LockSupport.unpark(worker);
  }

  @Override
  public void close() throws InterruptedException {
    synchronized (admission) {
      phase = Phase.CLOSED;
    }
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
    var completions = new SourceCompletions();
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
          commit(completions);
          synchronized (admission) {
            if (phase == Phase.RUNNING) {
              completions.dispatch(parallelism, sender);
            }
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
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
      }
      // SDK completes all admitted results before invoking close; never dispatch here.
      completions.collectCompleted();
      try {
        commit(completions);
      } catch (WakeupException shutdownWakeup) {
        // close may wake us between the final poll and commit. Retry this known wakeup only.
        commit(completions);
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

  private void commit(SourceCompletions completions) {
    var offsets = completions.committableOffsets();
    if (!offsets.isEmpty()) {
      consumer.commitSync(offsets);
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
        completions.markCommitted(offsets);
      }
      completions.remove(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
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
