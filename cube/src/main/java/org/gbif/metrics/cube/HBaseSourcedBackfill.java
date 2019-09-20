package org.gbif.metrics.cube;

import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Joiner;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.backfill.HBaseBackfill;
import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to help write backfillers that scan an HBase table as the input.
 */
public class HBaseSourcedBackfill {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSourcedBackfill.class);
  private static final String APPLICATION_PROPERTIES = "cube.properties";

  // Configuration that is read from the APPLICATION_PROPERTIES
  // We use bytes as they are most commonly used but some APIs require
  // them to be converted back to Strings. This is out of our hands though.
  private final byte[] cubeTable;
  private final byte[] snapshotTable;
  private final byte[] backfillTable;
  private final byte[] counterTable;
  private final byte[] lookupTable;
  private final byte[] cf;
  private final byte[] sourceTable;

  private final int scannerCache;
  private final int numReducers;
  private final int writeBatchSize;

  public static final int DEFAULT_NUM_REDUCERS = 12;
  public static final int DEFAULT_WRITE_BATCH_SIZE = 1000;
  public static final int DEFAULT_SCANNER_CACHE = 200;

  // Keys used for the application properties, and in the Hadoop context,
  // since that is the only way to pass things to the launched MR tasks.
  public static final String KEY_CUBE_TABLE = "cubeTable";
  public static final String KEY_SNAPSHOT_TABLE = "snapshotTable";
  public static final String KEY_BACKFILL_TABLE = "backfillTable";
  public static final String KEY_COUNTER_TABLE = "counterTable";
  public static final String KEY_LOOKUP_TABLE = "lookupTable";
  public static final String KEY_CF = "columnFamily";
  public static final String KEY_SOURCE_TABLE = "backfillSourceTable";
  public static final String KEY_SCANNER_CACHE = "backfillScannerCaching";
  public static final String KEY_HBASE_SCANNER_CACHE = "hbase.client.scanner.caching"; // for HBase job conf
  public static final String KEY_NUM_REDUCERS = "backfillNumReduceTasks";
  public static final String KEY_WRITE_BATCH_SIZE = "writeBatchSize";

  private final Properties props;


  public HBaseSourcedBackfill(String prefix) throws IllegalArgumentException, IOException {
    props = PropertiesUtil.loadProperties(APPLICATION_PROPERTIES);
    Joiner j = null;
    // handle complete or missing prefixes with no separator
    if (prefix == null || prefix.endsWith(".")) {
      j = Joiner.on("").skipNulls();
    } else {
      j = Joiner.on('.');
    }

    cubeTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_CUBE_TABLE), true, null);
    snapshotTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_SNAPSHOT_TABLE), true, null);
    backfillTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_BACKFILL_TABLE), true, null);
    counterTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_COUNTER_TABLE), false, null); // optional
    lookupTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_LOOKUP_TABLE), false, null); // optional
    cf = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_CF), true, null);
    sourceTable = PropertiesUtil.propertyAsUTF8Bytes(props, j.join(prefix, KEY_SOURCE_TABLE), true, null);
    scannerCache = PropertiesUtil.propertyAsInt(props, j.join(prefix, KEY_SCANNER_CACHE), false, HBaseSourcedBackfill.DEFAULT_SCANNER_CACHE);
    numReducers = PropertiesUtil.propertyAsInt(props, j.join(prefix, KEY_NUM_REDUCERS), false, DEFAULT_NUM_REDUCERS);
    writeBatchSize = PropertiesUtil.propertyAsInt(props, j.join(prefix, KEY_WRITE_BATCH_SIZE), false, DEFAULT_WRITE_BATCH_SIZE);
  }

  /**
   * Runs the backfill process.
   *
   * @throws IOException On any HBase communication errors
   */
  public void backfill(HBaseBackfillCallback callback, Class<? extends Deserializer<?>> deserializer) {
    Configuration conf = HBaseConfiguration.create();
    try {
      setup(conf);
      HBaseBackfill backfill = new HBaseBackfill(conf, callback, cubeTable, snapshotTable, backfillTable, cf, deserializer);
      backfill.runWithCheckedExceptions();
      cleanup(conf);

      LOG.info("Finished successfully");

    } catch (IOException e) {
      LOG.error("Error running cube backfill", e);
    }
  }

  /**
   * Removes the existing snapshot and backfill tables if present.
   */
  private void cleanup(Configuration conf) throws IOException {
    cleanup(new HBaseAdmin(conf));
  }

  /**
   * Removes the existing snapshot and backfill tables if present.
   */
  private void cleanup(HBaseAdmin admin) throws IOException {
    if (admin.tableExists(snapshotTable)) {
      LOG.info("Deleting table {}", Bytes.toString(snapshotTable));
      admin.disableTable(snapshotTable);
      admin.deleteTable(snapshotTable);
    }
    if (admin.tableExists(backfillTable)) {
      LOG.info("Deleting table {}", Bytes.toString(backfillTable));
      admin.disableTable(backfillTable);
      admin.deleteTable(backfillTable);
    }
  }

  // utility to create a table if it does not exist
  private void createIfMissing(HBaseAdmin admin, byte[] t) throws IOException {
    createIfMissing(admin, t, null, null, 0);
  }

  // utility to create a table if it does not exist
  private void createIfMissing(HBaseAdmin admin, byte[] t, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
    if (!admin.tableExists(t)) {
      LOG.info("Creating table {}", Bytes.toString(t));
      HColumnDescriptor cfDesc = new HColumnDescriptor(cf);
      //cfDesc.setBloomFilterType(BloomType.NONE);
      cfDesc.setMaxVersions(1);
      // TODO: http://dev.gbif.org/issues/browse/MET-7
      // cfDesc.setCompressionType(Algorithm.SNAPPY); // fails on the Snapshotter at the end
      HTableDescriptor tableDesc = new HTableDescriptor(t);
      tableDesc.addFamily(cfDesc);
      if (startKey != null && endKey != null && numRegions > 0) {
        admin.createTable(tableDesc, startKey, endKey, numRegions);
      } else {
        admin.createTable(tableDesc);
      }
    }
  }

  protected Properties getProperties() {
    return props;
  }

  /**
   * Removes the existing snapshot and backfill tables if present.
   * Creates the cube,counter and lookup tables if missing.
   * Sets the configuration options in the Hadoop context.
   */
  protected void setup(Configuration conf) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    cleanup(admin);
    createIfMissing(admin, cubeTable);

    if (counterTable != null) {
      createIfMissing(admin, counterTable);
      conf.set(KEY_COUNTER_TABLE, Bytes.toString(counterTable));
    }
    if (lookupTable != null) {
      createIfMissing(admin, lookupTable);
      conf.set(KEY_LOOKUP_TABLE, Bytes.toString(lookupTable));
    }

    // unfortunately we need to use Strings again (Hadoop API)
    conf.set(KEY_CUBE_TABLE, Bytes.toString(cubeTable));
    conf.set(KEY_SNAPSHOT_TABLE, Bytes.toString(snapshotTable));
    conf.set(KEY_BACKFILL_TABLE, Bytes.toString(backfillTable));
    conf.set(KEY_CF, Bytes.toString(cf));
    conf.set(KEY_SOURCE_TABLE, Bytes.toString(sourceTable));
    conf.setInt(KEY_HBASE_SCANNER_CACHE, scannerCache);
    conf.setInt(KEY_NUM_REDUCERS, numReducers);
    conf.setInt(KEY_WRITE_BATCH_SIZE, writeBatchSize);
  }
}
