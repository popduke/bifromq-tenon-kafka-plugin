package org.apache.bifromq.tenon.kafka;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Keeps one ordered attempt per partition and a snapshot awaiting Kafka commit. */
final class SourceCompletions {
  private static final int MAX_ATTEMPTS = 3;
  private final Map<TopicPartition, PartitionProgress> progress = new LinkedHashMap<>();

  void add(SourceRecordPayload payload) {
    var partition = new TopicPartition(payload.getTopic(), payload.getPartition());
    progress.computeIfAbsent(partition, ignored -> new PartitionProgress()).pending.add(payload);
  }

  void dispatch(int parallelism, PayloadSender<SourceRecordPayload> sender) {
    // debt: One in-flight record per partition bounds ordering complexity but throughput is
    // limited by acknowledgement latency. Introduce ordered admission windows only after profiling.
    progress.forEach(
        (partition, state) -> {
          if (!state.pending.isEmpty() && state.result == null) {
            int channel =
                Math.floorMod(
                    31 * partition.topic().hashCode() + partition.partition(), parallelism);
            state.result = sender.send(channel, state.pending.peek());
          }
        });
  }

  void collectCompleted() {
    for (var state : progress.values()) {
      if (state.result == null || !state.result.toCompletableFuture().isDone()) {
        continue;
      }
      switch (state.result.toCompletableFuture().join()) {
        case OK -> {
          state.committableOffset = state.pending.remove().getOffset() + 1;
          state.retries = 0;
        }
        case BACKPRESSURE -> {
          /* No admission occurred; wait for capacity until quiesce. */
        }
        case RETRY -> {
          if (++state.retries > MAX_ATTEMPTS) {
            throw new IllegalStateException("Kafka Source retry limit exceeded");
          }
        }
        case ERROR -> throw new IllegalStateException("Kafka Source record rejected by Tenon");
      }
      state.result = null;
    }
  }

  boolean hasPending() {
    return progress.values().stream().anyMatch(state -> !state.pending.isEmpty());
  }

  Set<TopicPartition> partitionsWithPending() {
    return progress.entrySet().stream()
        .filter(entry -> !entry.getValue().pending.isEmpty())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  Map<TopicPartition, OffsetAndMetadata> committableOffsets() {
    return committableOffsets(progress.keySet());
  }

  Map<TopicPartition, OffsetAndMetadata> committableOffsets(Collection<TopicPartition> partitions) {
    var offsets = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
    for (var partition : partitions) {
      var state = progress.get(partition);
      if (state != null && state.committableOffset != null) {
        offsets.put(partition, new OffsetAndMetadata(state.committableOffset));
      }
    }
    return offsets;
  }

  void markCommitted(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // All access, including commitSync, stays on the Consumer thread.
    offsets.keySet().forEach(partition -> progress.get(partition).committableOffset = null);
  }

  void remove(Collection<TopicPartition> partitions) {
    partitions.forEach(progress::remove);
  }

  private static final class PartitionProgress {
    private final ArrayDeque<SourceRecordPayload> pending = new ArrayDeque<>();
    private CompletionStage<AckCode> result;
    // Snapshot of the last OK, retained after its payload is removed and until commit succeeds.
    private Long committableOffset;
    private int retries;
  }
}
