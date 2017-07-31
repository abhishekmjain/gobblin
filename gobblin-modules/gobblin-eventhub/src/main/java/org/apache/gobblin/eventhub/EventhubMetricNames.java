package org.apache.gobblin.eventhub;

/**
 * Contains names for all metrics generated by eventhub component
 */
public class EventhubMetricNames {

  public static class EventhubDataWriterMetrics {

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number of records attempted
     * to be written by a {@link gobblin.eventhub.writer.EventhubDataWriter}. This includes retries.
     */
    public static final String RECORDS_ATTEMPTED_METER = "eventhub.writer.records.attempted";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number records written by a {@link gobblin.eventhub.writer.EventhubDataWriter}
     */
    public static final String RECORDS_SUCCESS_METER = "eventhub.writer.records.success";

    /** A {@link com.codahale.metrics.Meter} measuring the number of records
     * given to a {@link gobblin.eventhub.writer.EventhubDataWriter}. This does not count retries.
     */
    public static final String RECORDS_FAILED_METER = "eventhub.writer.records.failed";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number bytes written by a {@link gobblin.eventhub.writer.EventhubDataWriter} as
     */
    public static final String BYTES_WRITTEN_METER = "eventhub.writer.bytes.written";

    /**
     * A {@link com.codahale.metrics.Timer} measuring the time taken for each write operation.
     */
    public static final String WRITE_TIMER = "eventhub.writer.write.time";
  }
}
