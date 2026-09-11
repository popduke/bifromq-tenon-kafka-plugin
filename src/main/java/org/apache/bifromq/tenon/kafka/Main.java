package org.apache.bifromq.tenon.kafka;

import org.apache.bifromq.tenon.kafka.payload.SinkRecordPayload;
import org.apache.bifromq.tenon.kafka.payload.SourceRecordPayload;
import org.apache.bifromq.tenon.sdk.SourceAndSinkProgram;

/** Starts the Kafka Source-and-Sink Program and leaves lifecycle ownership to the Tenon SDK. */
public final class Main {
  private Main() {}

  static void main(String[] arguments) throws Exception {
    var program =
        SourceAndSinkProgram.<SourceRecordPayload, SinkRecordPayload>run(
            arguments, SinkRecordPayload.parser());
    program.createSource().start();
    program.awaitShutdown();
  }
}
