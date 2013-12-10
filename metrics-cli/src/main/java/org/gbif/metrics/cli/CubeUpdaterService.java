package org.gbif.metrics.cli;

import org.gbif.common.messaging.MessageListener;

import java.io.File;

import com.google.common.util.concurrent.AbstractIdleService;
import com.urbanairship.datacube.Op;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A base class for services that will mutate cubes from occurrences.
 *
 * @param <T> The configuration type
 */
abstract class CubeUpdaterService<T extends CubeConfiguration> extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(CubeUpdaterService.class);
  // number of IOExceptions we'll tolerate before dying
  public static final int DEFAULT_IOE_RETRIES = 10000;
  private final T configuration;
  private MessageListener listener;

  protected CubeUpdaterService(T configuration) {
    this.configuration = configuration;
  }

  abstract CubeUpdaterCallback<? extends Op> getCallback(T configuration, Configuration hadoopConfiguration);

  @Override
  protected void startUp() throws Exception {
    File hbaseConfig = new File(configuration.hbaseConfig);
    checkArgument(hbaseConfig.exists() && hbaseConfig.isFile(), "hbase-site.xml does not exist");
    Configuration hadoopConfiguration = new Configuration();
    hadoopConfiguration.addResource(hbaseConfig.toURI().toURL());

    configuration.ganglia.start();
    CubeUpdaterCallback<? extends Op> callback = getCallback(configuration, hadoopConfiguration);
    callback.startInactiveWatcher(); // tell it to flush on periods of inactivity

    LOG.info("Starting cube service with {} queue listeners merging batches into sizes of {} before handing over to {} threads flushing to HBase",
      configuration.messageConsumerThreads, configuration.batchSize, configuration.batchFlushThreads);
    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName, configuration.messageConsumerThreads, callback);
  }
}
