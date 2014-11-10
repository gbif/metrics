package org.gbif.metrics.cube;


import org.gbif.utils.file.properties.PropertiesUtil;

import java.util.Properties;

import com.google.common.base.Joiner;
import com.google.inject.AbstractModule;

/**
 * Utility for reading necessary properties for the guice modules.
 */
public abstract class CubeHBaseModule extends AbstractModule {

  private final byte[] cubeTable;
  private final byte[] counterTable;
  private final byte[] lookupTable;
  private final byte[] cf;
  private final int writeBatchSize;

  public CubeHBaseModule(Properties props, String prefix) {
    Joiner j = Joiner.on(".");
    cubeTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, HBaseSourcedBackfill.KEY_CUBE_TABLE), true, null);
    counterTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, HBaseSourcedBackfill.KEY_COUNTER_TABLE), false, null); // optional
    lookupTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, HBaseSourcedBackfill.KEY_LOOKUP_TABLE), false, null);  // optional
    cf = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, HBaseSourcedBackfill.KEY_CF), true, null);
    writeBatchSize = PropertiesUtil.propertyAsInt(props, j.join(prefix, HBaseSourcedBackfill.KEY_WRITE_BATCH_SIZE), true, null);
  }

  protected byte[] getCf() {
    return cf;
  }

  protected byte[] getCounterTable() {
    return counterTable;
  }

  protected byte[] getCubeTable() {
    return cubeTable;
  }

  protected byte[] getLookupTable() {
    return lookupTable;
  }

  protected int getWriteBatchSize() {
    return writeBatchSize;
  }
}
