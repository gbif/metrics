package org.gbif.metrics.cli;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

/**
 * A typical minimum configuration for cube mutators capturing the messaging and HBase environments, and the usual
 * tables required in a cube.
 */
class CubeConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();
  
  @ParametersDelegate
  @Valid
  @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();  

  @Parameter(names = "--messaging-thread-count", description = "The number of threads (channels) consuming from the messaging server")
  @Min(1)
  public int messageConsumerThreads = 5;

  @Parameter(names = "--messaging-queue-name")
  @NotNull
  public String queueName = "metrics_updater";

  @Parameter(names = "--batch-size", description = "The number of ops (post merging) to handoff to a flushing thread")
  @Min(1)
  public int batchSize = 1000;

  @Parameter(names = "--batch-flush-threads", description = "The number of threads that write batches to HBase")
  @Min(1)
  public int batchFlushThreads = 1;

  @Parameter(names = "--hbase-conf", description = "Specify the location of the hbase-site.xml file")
  @NotNull
  public String hbaseConfig;

  @Parameter(names = "--cube-table")
  @NotNull
  public String cubeTable;

  @Parameter(names = "--cube-lookup-table")
  @Nullable
  public String cubeLookupTable;

  @Parameter(names = "--cube-counter-table")
  @Nullable
  public String cubeCounterTable;

  @Parameter(names = "--column-family")
  @NotNull
  public String columnFamily;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("messageConsumerThreads", messageConsumerThreads)
      .add("queueName", queueName).add("batchSize", batchSize).add("hbaseConfig", hbaseConfig)
      .add("cubeTable", cubeTable).add("cubeLookupTable", cubeLookupTable).add("cubeCounterTable", cubeCounterTable)
      .add("columnFamily", columnFamily).add("batchFlushThreads", batchFlushThreads).toString();
  }
}
