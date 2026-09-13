package org.apache.bifromq.tenon.kafka;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Keeps ordered per-partition records and a snapshot awaiting Kafka commit. */
final class SourceCompletions {
  private static final int MAX_ATTEMPTS = 3;
  private final Map<TopicPartition, PartitionProgress> progress = new LinkedHashMap<>();
  private final ArrayDeque<RecordState> ready = new ArrayDeque<>();
  private final ConcurrentLinkedQueue<RecordState> completed = new ConcurrentLinkedQueue<>();
  private final Runnable signal;

  SourceCompletions(Runnable signal) {
    this.signal = signal;
  }

  void add(SourceRecordPayload payload) {
    var partition = new TopicPartition(payload.getTopic(), payload.getPartition());
    var record = new RecordState(payload);
    progress.computeIfAbsent(partition, ignored -> new PartitionProgress()).pending.add(record);
    ready.add(record);
  }

  void dispatch(
      int parallelism, PayloadSender<SourceRecordPayload> sender, BooleanSupplier accepting) {
    // Admission permits bound the number of records in flight; completion order does not affect
    // the continuous offset prefix committed for this partition. The ready queue means each
    // record is considered only when it is new or its previous attempt completed.
    while (!ready.isEmpty()) {
      var record = ready.poll();
      if (record == null || record.discarded || record.done || record.result != null) {
        continue;
      }
      if (!accepting.getAsBoolean()) {
        ready.addFirst(record);
        return;
      }
      var partition = record.partition;
      int channel =
          Math.floorMod(31 * partition.topic().hashCode() + partition.partition(), parallelism);
      record.result = sender.send(channel, record.payload);
      record.result.whenComplete(
          (ignored, error) -> {
            completed.add(record);
            signal.run();
          });
    }
  }

  void collectCompleted() {
    RecordState record;
    while ((record = completed.poll()) != null) {
      if (record.discarded || record.result == null) {
        continue;
      }
      switch (record.result.toCompletableFuture().join()) {
        case OK -> record.done = true;
        case BACKPRESSURE -> {
          /* No admission occurred; retry this record in a later cycle. */
        }
        case RETRY -> {
          if (++record.retries > MAX_ATTEMPTS) {
            throw new IllegalStateException("Kafka Source retry limit exceeded");
          }
        }
        case ERROR -> throw new IllegalStateException("Kafka Source record rejected by Tenon");
      }
      record.result = null;
      if (!record.done) {
        ready.add(record);
      }
    }
    for (var state : progress.values()) {
      while (!state.pending.isEmpty() && state.pending.peek().done) {
        state.committableOffset = state.pending.remove().payload.getOffset() + 1;
      }
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
    // Only clear the exact snapshot that was acknowledged. A newer completed offset may have
    // been produced while an older async commit was in flight.
    offsets.forEach(
        (partition, offset) -> {
          var state = progress.get(partition);
          if (state != null && offset.offset() == state.committableOffset) {
            state.committableOffset = null;
          }
        });
  }

  void remove(Collection<TopicPartition> partitions) {
    for (var partition : partitions) {
      var state = progress.remove(partition);
      if (state != null) {
        state.pending.forEach(record -> record.discarded = true);
      }
    }
  }

  private static final class PartitionProgress {
    private final ArrayDeque<RecordState> pending = new ArrayDeque<>();
    // Snapshot of the last OK, retained after its payload is removed and until commit succeeds.
    private Long committableOffset;
  }

  private static final class RecordState {
    private final SourceRecordPayload payload;
    private final TopicPartition partition;
    private CompletionStage<AckCode> result;
    private int retries;
    private boolean done;
    private boolean discarded;

    private RecordState(SourceRecordPayload payload) {
      this.payload = payload;
      this.partition = new TopicPartition(payload.getTopic(), payload.getPartition());
    }
  }
}
