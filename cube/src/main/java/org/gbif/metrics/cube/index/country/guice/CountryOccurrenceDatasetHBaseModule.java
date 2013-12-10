package org.gbif.metrics.cube.index.country.guice;

import org.gbif.metrics.cube.CubeHBaseModule;
import org.gbif.metrics.cube.CubeIo;
import org.gbif.metrics.cube.CubeIo.Type;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.index.common.UuidIntMap;
import org.gbif.metrics.cube.index.country.OccurrenceDatasetCountryCube;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.urbanairship.datacube.DataCubeIo;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * Sets up the CubeIO.
 */
public class CountryOccurrenceDatasetHBaseModule extends CubeHBaseModule {

  private static final String PREFIX = "country.occurrence_dataset";
  private HBaseCubes<UuidIntMap> hbaseCube;

  public CountryOccurrenceDatasetHBaseModule(Properties props) {
    super(props, PREFIX);
  }

  @Singleton
  @Provides
  @CubeIo(Type.COUNTRY_OCCURRRENCE_DATASET)
  public DataCubeIo<UuidIntMap> getHBaseCube() throws IOException {
    return hbaseCube.getDataCubeIo();
  }

  @Override
  protected void configure() {
    try {
      hbaseCube =
        HBaseCubes.newCombiningBatchAsync(OccurrenceDatasetCountryCube.INSTANCE, UuidIntMap.DESERIALIZER,
          getCubeTable(),
          getCf(), HBaseConfiguration.create(), getWriteBatchSize());
    } catch (IOException e) {
      this.addError(e);
    }
  }
}
