# BifroMQ Tenon Kafka Plugin

A Tenon Java Plugin that provides Kafka Source and Sink capabilities in one `source-and-sink` program.

The plugin uses the official Apache Kafka Java client:

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>4.3.1</version>
</dependency>
```

## For users

Configure one plugin instance with Kafka connection details, an explicit consumer group, and one or more source topics. `sourceConsumerCount` controls how many Kafka consumers join that group and defaults to one. Sink records may provide their own topic or use `defaultTopic`.

The Source disables automatic Kafka offset commits. It commits the next offset only after all earlier records in the same partition have completed successfully in Tenon. Normal commits are asynchronous; rebalance and shutdown use synchronous commits to close the final acknowledged boundary. The Sink completes a batch only after Kafka acknowledges every record. The resulting delivery guarantee is at-least-once; a process or connection failure can cause records to be delivered again.

The configuration schema is [src/main/tenon/config.schema.json](src/main/tenon/config.schema.json). Payload definitions are [source_record_payload.proto](src/main/proto/source_record_payload.proto) and [sink_record_payload.proto](src/main/proto/sink_record_payload.proto).

## Source parallelism

`parallelism` is the number of ordered Tenon Flow channels configured for a Flow. It is not the number of Kafka consumers, producer connections, or worker threads.

The plugin creates `sourceConsumerCount` Kafka consumer workers. They use the same explicit `groupId` and subscribe to the same topics, so Kafka assigns each topic-partition to at most one worker at a time. For every Kafka topic-partition, the plugin computes a stable hash of the topic name and partition number and maps that partition to a channel in the range `0 .. parallelism - 1`. Records from the same Kafka partition always use the same channel, even after consumer-group rebalance. Different Kafka partitions may use different channels and can progress independently. Multiple records from one partition may be in flight while Tenon admission permits are available; offset commits still advance only across the continuous successful prefix. Rebalance may replay records that were not committed.

Increasing Flow `parallelism` gives the pipeline more independent channels, but it does not create more Kafka consumers. Kafka consumer-group scaling within one Plugin instance is controlled by `sourceConsumerCount`; all of those consumers share the configured `groupId`.

## For maintainers

The project root is the Maven project. Main classes are under `src/main/java/org/apache/bifromq/tenon/kafka`:

- `KafkaPlugin` owns the shared Kafka producer and creates Sources.
- `KafkaSource` owns one Kafka consumer and its worker thread.
- `SourceCompletions` tracks per-partition ordering, offset commits, backpressure, and bounded retries.
- `KafkaSink` maps payloads to Kafka producer records and waits for the complete batch.
- `Main` and `SourceAndSinkPluginFactory` provide the Tenon process entry point.

Build the project with Maven:

```shell
./mvnw spotless:apply verify
```

The build needs the Tenon SDK and Maven Plugin artifacts available to Maven. It does not require a checkout of the Tenon source repository.

The build runs unit tests and creates self-contained bundles for the platforms declared in the Maven Plugin configuration under `target/`:

```text
target/tenon-kafka-source-and-sink-0.1.0-tenon-plugin-<platform>.tar.gz
```

The bundle includes Eclipse Temurin Java 25, the plugin, Kafka Client 4.3.1, and runtime dependencies. It always starts with `runtime/bin/java`; `JAVA_HOME` and an externally installed JVM are neither read nor required.
