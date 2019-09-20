package org.gbif.metrics.cube.index.taxon.guice;

import org.gbif.metrics.cube.CubeHBaseModule;
import org.gbif.metrics.cube.CubeIo;
import org.gbif.metrics.cube.CubeIo.Type;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.index.common.UuidIntMap;
import org.gbif.metrics.cube.index.taxon.OccurrenceDatasetCube;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.urbanairship.datacube.DataCubeIo;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * Sets up the CubeIO.
 */
public class TaxonOccurrenceDatasetHBaseModule extends CubeHBaseModule {

  private static final String PREFIX = "taxon.occurrence_dataset";
  private HBaseCubes<UuidIntMap> hbaseCube;

  public TaxonOccurrenceDatasetHBaseModule(Properties props) {
    super(props, PREFIX);
  }

  @Override
  protected void configure() {
    try {
      hbaseCube =
        HBaseCubes.newCombiningBatchAsync(OccurrenceDatasetCube.INSTANCE, UuidIntMap.DESERIALIZER, getCubeTable(),
          getCf(), HBaseConfiguration.create(), getWriteBatchSize());
    } catch (IOException e) {
      this.addError(e);
    }
  }

  @Singleton
  @Provides
  @CubeIo(Type.TAXON_OCCURRENCE_DATASET)
  public DataCubeIo<UuidIntMap> getHBaseCube() throws IOException {
    return hbaseCube.getDataCubeIo();
  }
}
