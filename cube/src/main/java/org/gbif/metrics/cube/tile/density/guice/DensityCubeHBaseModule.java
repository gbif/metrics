package org.gbif.metrics.cube.tile.density.guice;

import org.gbif.metrics.cube.CubeHBaseModule;
import org.gbif.metrics.cube.CubeIo;
import org.gbif.metrics.cube.CubeIo.Type;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.tile.density.DensityCube;
import org.gbif.metrics.cube.tile.density.DensityTile;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.urbanairship.datacube.DataCubeIo;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * Sets up the CubeIO.
 */
public class DensityCubeHBaseModule extends CubeHBaseModule {

  private static final String PREFIX = "density-cube";
  private HBaseCubes<DensityTile> hbaseCube;

  public DensityCubeHBaseModule(Properties props) {
    super(props, PREFIX);
  }

  @Override
  protected void configure() {
    try {
      hbaseCube =
        HBaseCubes.newCombiningBatchAsync(DensityCube.INSTANCE, DensityTile.DESERIALIZER, getCubeTable(),
          getCf(), HBaseConfiguration.create(), getWriteBatchSize());
    } catch (IOException e) {
      this.addError(e);
    }
  }

  @Singleton
  @Provides
  public DataCubeIo<DensityTile> getHBaseCube() throws IOException {
    return hbaseCube.getDataCubeIo();
  }
}
