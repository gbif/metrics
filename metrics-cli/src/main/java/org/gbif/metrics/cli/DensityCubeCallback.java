package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.tile.density.DensityCube;
import org.gbif.metrics.cube.tile.density.DensityCubeUtil;
import org.gbif.metrics.cube.tile.density.DensityCubeUtil.Op;
import org.gbif.metrics.cube.tile.density.DensityTile;

import java.io.IOException;

import com.google.common.base.Throwables;
import com.urbanairship.datacube.AsyncException;
import com.urbanairship.datacube.Batch;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The callback for handling messages for density tile updates.
 *
 * NOTE: It is intended that only 1 be registered per queue.
 *
 * This configures an asynchronously flushing datacube using say N threads and a batch size of S.
 * To ensure no contention, this will build a buffer of updates of size N*(S+1) before paritioning
 * into N batches of size (S+1) and writing to DataCube.  By doing this, we know that datacube will
 * flush them immediately.
 *
 * This is designed so that we can have numerous HBase flushing threads, which is required for any
 * kind of performance, but ensure that they will never compete for updating cells.
 *
 * This class is threadsafe and stateful, but not intended to be used in multithreaded manner as it
 * uses synchronized locks.
 */
public class DensityCubeCallback extends CubeUpdaterCallback<DensityTile> {
  private static final Logger LOG = LoggerFactory.getLogger(DensityCubeCallback.class);
  private final Batch<DensityTile> bufferBatch = new Batch<DensityTile>();
  private final Object lock = new Object(); // for the batch
  private final DensityCubeConfiguration configuration;
  private final HBaseCubes<DensityTile> cube;

  public DensityCubeCallback(DensityCubeConfiguration configuration, Configuration hadoopConfiguration) {
    this.configuration = configuration;
    try {
      cube =
        HBaseCubes.newCombiningBatchAsync(DensityCube.INSTANCE, DensityTile.DESERIALIZER,
          configuration.cubeTable.getBytes(), configuration.columnFamily.getBytes(), hadoopConfiguration,
          configuration.batchSize, configuration.batchFlushThreads, CubeUpdaterService.DEFAULT_IOE_RETRIES, Integer.MAX_VALUE);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Closes the underlying datacube instance.
   */
  public void close() {
    synchronized (lock) {
      flushBatch(bufferBatch);
    }
    cube.closeQuietly();
  }

  /**
   * Hands the update batch to datacube.
   */
  private void flushBatch(Batch<DensityTile> update) {
    try {
      if (update.getMap() != null && !update.getMap().isEmpty()) {
        cube.write(update);
      }
    } catch (AsyncException e) {
      LOG.error("AsyncException while updating cube with a batch size of {}", update.getMap().size(), e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while updating cube with a batch size of {}", update.getMap().size(), e);
    }
  }

  @Override
  HBaseCubes<DensityTile> getCube() {
    return cube;
  }

  @Override
  protected Batch<DensityTile> getNewMutations(Occurrence occurrence) {
    return DensityCubeUtil.cubeMutations(occurrence, Op.ADDITION, configuration.zoom,
      configuration.pixelsPerCluster);
  }

  @Override
  protected Batch<DensityTile> getUpdateMutations(Occurrence oldOccurrence, Occurrence newOccurrence) {
    Batch<DensityTile> update =
      DensityCubeUtil.cubeMutations(oldOccurrence, Op.SUBTRACTION, configuration.zoom,
        configuration.pixelsPerCluster);
    if (newOccurrence != null) { // a delete has no new occurrence
      update.putAll(DensityCubeUtil.cubeMutations(newOccurrence, Op.ADDITION, configuration.zoom,
        configuration.pixelsPerCluster));
    }
    return update;
  }
}
