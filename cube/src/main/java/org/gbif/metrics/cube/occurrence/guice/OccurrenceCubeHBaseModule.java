package org.gbif.metrics.cube.occurrence.guice;

import org.gbif.metrics.cube.CubeHBaseModule;
import org.gbif.metrics.cube.CubeIo;
import org.gbif.metrics.cube.CubeIo.Type;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.occurrence.OccurrenceCube;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.ops.LongOp.LongOpDeserializer;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * Sets up the CubeIO.
 */
public class OccurrenceCubeHBaseModule extends CubeHBaseModule {

  private static final String PREFIX = "occurrence-cube";
  private HBaseCubes<LongOp> hbaseCube;

  public OccurrenceCubeHBaseModule(Properties props) {
    super(props, PREFIX);
  }

  @Override
  protected void configure() {
    try {
      hbaseCube =
        HBaseCubes.newCombiningBatchAsync(OccurrenceCube.INSTANCE, new LongOpDeserializer(), getCubeTable(),
          getCf(), HBaseConfiguration.create(), getWriteBatchSize());
    } catch (IOException e) {
      this.addError(e);
    }
  }

  @Singleton
  @Provides
  @CubeIo(Type.OCCURRENCE)
  public DataCubeIo<LongOp> getHBaseCube() throws IOException {
    return hbaseCube.getDataCubeIo();
  }
}
