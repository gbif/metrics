package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.index.common.UuidIntMap;

import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.WriteBuilder;
import org.apache.hadoop.conf.Configuration;

/**
 * Generic service to update a Cube with the form: Key of type T and value UuidIntMap.
 * This class requires the following parameters:
 * - CubeConfiguration: service configuration.
 * - DataCube<UuidIntMap>: data cube reference that will be updated.
 * - Dimension<T>: this service updates 1 dimension only, T is the type of the column that holds the dimension value.
 * - Function<Occurrence, T>: functions that reads the dimension value from an occurrence record.
 */
class OccurrenceUuidIntMapCubeUpdaterService<T> extends CubeUpdaterService<CubeConfiguration> {
  // reduce reuse recycle HBase connections
  private volatile HBaseCubes<UuidIntMap> singletonCube;
  private final DataCube<UuidIntMap> dataCube;
  private final Dimension<T> dimension;
  private final Function<Occurrence, T> valueGetter;

  public OccurrenceUuidIntMapCubeUpdaterService(CubeConfiguration configuration, DataCube<UuidIntMap> dataCube,
    Dimension<T> dimension, Function<Occurrence, T> valueGetter) {
    super(configuration);
    this.dataCube = dataCube;
    this.dimension = dimension;
    this.valueGetter = valueGetter;
  }

  @Override
  CubeUpdaterCallback<UuidIntMap> getCallback(final CubeConfiguration configuration,
    final Configuration hadoopConfiguration) {

    return new CubeUpdaterCallback<UuidIntMap>() {
      /**
       * Creates the a mutation, this service adds/subtracts the count of datasets depending of the parameter incDec.
       */
      private Batch<UuidIntMap> cubeMutation(final Occurrence occurrence, final boolean isIncrement) {
        Batch<UuidIntMap> update = new Batch<UuidIntMap>();
        final T coordinate = valueGetter.apply(occurrence);
        final int incDec = isIncrement ? 1 : -1;
        try {
          if (coordinate != null && occurrence.getDatasetKey() != null) {
            UuidIntMap mapValue =
              UuidIntMap.newInstance(ImmutableMap.<UUID, Integer>of(occurrence.getDatasetKey(), incDec));
            WriteBuilder wb = new WriteBuilder(dataCube).at(dimension, coordinate);
            update.putAll(dataCube.getWrites(wb, mapValue).getMap());
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return update;
      }

      @Override
      protected synchronized HBaseCubes<UuidIntMap> getCube() {
        if (singletonCube == null) {
          try {
            singletonCube =
              HBaseCubes.newCombiningBatchAsync(dataCube, UuidIntMap.DESERIALIZER,
                configuration.cubeTable.getBytes(), null, null, configuration.columnFamily.getBytes(), hadoopConfiguration,
                configuration.batchSize, configuration.batchFlushThreads, DEFAULT_IOE_RETRIES, Integer.MAX_VALUE);

          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
        return singletonCube;
      }

      @Override
      protected Batch<UuidIntMap> getNewMutations(Occurrence occurrence) {
        return cubeMutation(occurrence, true);

      }

      @Override
      protected Batch<UuidIntMap> getUpdateMutations(Occurrence oldOccurrence, Occurrence newOccurrence) {
        Batch<UuidIntMap> update = cubeMutation(oldOccurrence, false);
        // newOccurrence will be null for deletes
        if (newOccurrence != null) {
          update.putAll(cubeMutation(newOccurrence, true));
        }

        return update;
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
