package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.occurrence.OccurrenceAddressUtil;
import org.gbif.metrics.cube.occurrence.OccurrenceCube;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.conf.Configuration;

/**
 * The service to update the occurrence cube.
 */
class OccurrenceCubeUpdaterService extends CubeUpdaterService<CubeConfiguration> {
  // reduce reuse recycle HBase connections
  private volatile HBaseCubes<LongOp> singletonCube;

  public OccurrenceCubeUpdaterService(CubeConfiguration configuration) {
    super(configuration);
  }

  /**
   * @return The same batch with zero values omitted
   */
  @VisibleForTesting
  static Batch<LongOp> trimZeros(Batch<LongOp> batch) {
    Iterator<Map.Entry<Address, LongOp>> iter = batch.getMap().entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Address, LongOp> entry = iter.next();
      if (0 == entry.getValue().getLong()) {
        iter.remove();
      }
    }
    return batch;
  }

  @Override
  CubeUpdaterCallback<LongOp>
    getCallback(final CubeConfiguration configuration,
    final Configuration hadoopConfiguration) {

    return new CubeUpdaterCallback<LongOp>() {

      @Override
      synchronized HBaseCubes<LongOp> getCube() {
        if (singletonCube == null) {
          try {
            singletonCube =
              HBaseCubes.newIncrementingBatchAsync(OccurrenceCube.INSTANCE, LongOp.DESERIALIZER,
                configuration.cubeTable.getBytes(), configuration.columnFamily.getBytes(), hadoopConfiguration,
                configuration.batchSize, configuration.batchFlushThreads);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
        return singletonCube;
      }

      @Override
      protected Batch<LongOp> getNewMutations(Occurrence occurrence) {
        return OccurrenceAddressUtil.cubeMutation(occurrence, new LongOp(1));
      }

      @Override
      protected Batch<LongOp> getUpdateMutations(Occurrence oldOccurrence, Occurrence newOccurrence) {
        Batch<LongOp> update = OccurrenceAddressUtil.cubeMutation(oldOccurrence, new LongOp(-1));
        // newOccurrence will be null for deletes
        if (newOccurrence != null) {
          update.putAll(OccurrenceAddressUtil.cubeMutation(newOccurrence, new LongOp(1)));
        }
        // adding a 1 to a -1 is common, and we know that is 0, so let's flush that out now
        return trimZeros(update);
      }
    };
  }

  @Override
  protected void shutDown() throws Exception {
    if (singletonCube != null) {
      singletonCube.close();
    }
  }
}
