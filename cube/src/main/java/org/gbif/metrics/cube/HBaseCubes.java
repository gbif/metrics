/**
 * 
 */
package org.gbif.metrics.cube;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.urbanairship.datacube.AsyncException;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.WriteBuilder;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a utility class to simplify dealing with the HBase CubeIo IO.
 * Depending on usage, sometimes you need access to the underlying Pool of HTables, to control for example auto
 * flushing. This class is intended to simplify those operations by providing a simpler API than those provided
 * by the underlying HBase and DataCube libraries.
 * There are 2 key scenarios typically you want to run DataCube in:
 * <ol>
 * <li>In a backfill to populate a new table, where certain optimizations can be made</li>
 * <li>In live read write mode</li>
 * </ol>
 * Note: Decision was to use separate table per cube and per lookup (to enable easy truncating by cube), hence
 * uniqueCubeName is empty in the idService and the hbaseHarness. This is a decision that might be revisited in the
 * future.
 */
public class HBaseCubes<O extends Op> {

  public static final int DEFAULT_FLUSH_THREADS = 10;
  public static final int DEFAULT_IOE_RETRIES = 10000;
  public static final int DEFAULT_CAS_RETRIES = 10000;

  private static Logger LOG = LoggerFactory.getLogger(HBaseCubes.class);
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private final DataCubeIo<O> dataCubeIo;
  private final HTablePool pool;
  private final byte[] cubeTable;
  private final SyncLevel syncLevel;

  private HBaseCubes(DataCube<O> cube, Deserializer<O> deserializer, byte[] cubeTable, byte[] lookupTable,
    byte[] counterTable, byte[] cf, Configuration conf, int writeBatchSize, CommitType commitType, SyncLevel syncLevel,
    HTableFactory factory) throws IOException {
    this(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize, commitType, syncLevel,
      factory, DEFAULT_FLUSH_THREADS, DEFAULT_IOE_RETRIES, DEFAULT_CAS_RETRIES);
  }

  private HBaseCubes(DataCube<O> cube, Deserializer<O> deserializer, byte[] cubeTable, byte[] lookupTable,
    byte[] counterTable, byte[] cf, Configuration conf, int writeBatchSize, CommitType commitType, SyncLevel syncLevel,
    HTableFactory factory, int numFlushThreads, int numIoeRetries, int numCasRetries) throws IOException {
    this.cubeTable = cubeTable;
    this.syncLevel = syncLevel;
    pool = new HTablePool(conf, 10, factory);
    IdService idService = new HBaseIdService(conf, lookupTable, counterTable, cf, EMPTY_BYTE_ARRAY);
    DbHarness<O> hbaseDbHarness =
      new HBaseDbHarness<O>(pool, EMPTY_BYTE_ARRAY, cubeTable, cf, deserializer, idService, commitType,
        numFlushThreads, numIoeRetries, numCasRetries, null); // these are the defaults
    // Note: batches are flushed after 1 minute
    dataCubeIo = new DataCubeIo<O>(cube, hbaseDbHarness, writeBatchSize, TimeUnit.MINUTES.toMillis(1), syncLevel);
  }

  /**
   * The "no counter / lookup" version.
   */
  private HBaseCubes(DataCube<O> cube, Deserializer<O> deserializer, byte[] cubeTable, byte[] cf, Configuration conf,
    int writeBatchSize, CommitType commitType, SyncLevel syncLevel, HTableFactory factory) throws IOException {
    this(cube, deserializer, cubeTable, null, null, cf, conf, writeBatchSize, commitType, syncLevel, factory);
  }


  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by reading, merging and then
   * doing a check and store.
   */
  public static <O extends Op> HBaseCubes<O> newCombiningBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] lookupTable, byte[] counterTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize,
      CommitType.READ_COMBINE_CAS, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by reading, merging and then
   * doing a check and store.
   */
  public static <O extends Op> HBaseCubes<O> newCombiningBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] lookupTable, byte[] counterTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize, int numFlushThreads, int numIoeRetries, int numCasRetries) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize,
      CommitType.READ_COMBINE_CAS, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory(), numFlushThreads,
      numIoeRetries, numCasRetries);
  }

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by reading, merging and then
   * doing a check and store.
   */
  public static <O extends Op> HBaseCubes<O> newCombiningBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize, int numFlushThreads, int numIoeRetries, int numCasRetries) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, null, null, cf, conf, writeBatchSize,
      CommitType.READ_COMBINE_CAS, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory(), numFlushThreads,
      numIoeRetries, numCasRetries);
  }

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by reading, merging and then
   * doing a check and store.
   */
  public static <O extends Op> HBaseCubes<O> newCombiningBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] cf, @Nullable Configuration conf, int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, cf, conf, writeBatchSize, CommitType.READ_COMBINE_CAS,
      SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by doing an increment call.
   * This is therefore ONLY suitable for Op that supports increment, such as IntOp and LongOp.
   */
  public static <O extends Op> HBaseCubes<O> newIncrementingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] cf, @Nullable Configuration conf, int writeBatchSize, int numFlushThreads) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, null, null, cf, conf, writeBatchSize,
      CommitType.INCREMENT, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory(), numFlushThreads, 
      DEFAULT_IOE_RETRIES, DEFAULT_CAS_RETRIES);
  }  
  

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by doing an increment call.
   * This is therefore ONLY suitable for Op that supports increment, such as IntOp and LongOp.
   */
  public static <O extends Op> HBaseCubes<O> newIncrementingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] lookupTable, byte[] counterTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize,
      CommitType.INCREMENT, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }
  

  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by doing an increment call.
   * This is therefore ONLY suitable for Op that supports increment, such as IntOp and LongOp.
   */
  public static <O extends Op> HBaseCubes<O> newIncrementingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] lookupTable, byte[] counterTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize, int numFlushThreads) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize,
      CommitType.INCREMENT, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory(), numFlushThreads, DEFAULT_IOE_RETRIES, DEFAULT_CAS_RETRIES);
  }  
  
  /**
   * Creates a new batch writing instance that will modify any existing data in the cube by doing an increment call.
   * This is therefore ONLY suitable for Op that supports increment, such as IntOp and LongOp.
   */
  public static <O extends Op> HBaseCubes<O> newIncrementingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] cf, @Nullable Configuration conf, int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, cf, conf, writeBatchSize, CommitType.INCREMENT,
      SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }

  // Utility to create an HTableFactory that disables auto flushing
  private static HTableFactory newNonFlushingHTableFactory() {
    return new HTableFactory() {

      @Override
      public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
          HTable table = new HTable(config, tableName);
          table.setAutoFlush(false);
          return table;
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
  }

  /**
   * Creates a new batch writing instance that will overwrite any existing data in the cube.
   * Note that following writes, clients must call closeQuietly(), or close() in order to flush all pending commits to
   * HBase. Particularly suited to backfill tasks.
   */
  public static <O extends Op> HBaseCubes<O> newOverwritingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] lookupTable, byte[] counterTable, byte[] cf, @Nullable Configuration conf,
    int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, lookupTable, counterTable, cf, conf, writeBatchSize,
      CommitType.OVERWRITE, SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }

  /**
   * Creates a new batch writing instance that will overwrite any existing data in the cube.
   * Note that following writes, clients must call closeQuietly(), or close() in order to flush all pending commits to
   * HBase. Particularly suited to backfill tasks.
   */
  public static <O extends Op> HBaseCubes<O> newOverwritingBatchAsync(DataCube<O> cube, Deserializer<O> deserializer,
    byte[] cubeTable, byte[] cf, @Nullable Configuration conf, int writeBatchSize) throws IOException {
    conf = (conf != null) ? conf : new Configuration();
    return new HBaseCubes<O>(cube, deserializer, cubeTable, cf, conf, writeBatchSize, CommitType.OVERWRITE,
      SyncLevel.BATCH_ASYNC, newNonFlushingHTableFactory());
  }

  /**
   * Closes the underlying connections, flushing etc. Following this no more operations will function.
   */
  public void close() throws IOException, InterruptedException {
    try {
      if (pool != null) {
        flush();
        pool.closeTablePool(cubeTable);
      }
    } catch (NullPointerException e) {
      // thrown when the pool has never issued this table, indicating an error
      throw new IOException("Attempt to close HBase pool, when table has not been written to.");
    }
  }

  /**
   * Closes the underlying connections, flushing etc. Following this no more operations will function.
   */
  public void closeQuietly() {
    try {
      close();
    } catch (Exception e) {
      LOG.warn("Attempt to close HBase pool, when table has not been written to.  Ignoring");
    }
  }

  /**
   * Flushed the cube IO.
   */
  public void flush() throws InterruptedException {
    dataCubeIo.flush();
  }

  /**
   * Provide the underlying cubeIO instance.
   */
  public DataCubeIo<O> getDataCubeIo() {
    return dataCubeIo;
  }

  /**
   * Utility to write a batch to the cube.
   * NOTE: This is for advanced users only, and requires an understanding of DataCube internals.
   * For example, one must know that this ONLY works for the Batch_Async mode.
   * 
   * @param batch To perform
   * @throws IllegalStateException Should this be called with the wrong sync level
   */
  public void write(Batch<O> batch) throws IllegalStateException, AsyncException, InterruptedException {
    Preconditions.checkState(SyncLevel.BATCH_ASYNC == syncLevel);
    dataCubeIo.writeAsync(batch);
  }

  /**
   * Utility to simplify writes, by calling the appropriate write or writeAsync
   * 
   * @throws AsyncException only if using the ASync mode
   */
  public void write(O op, WriteBuilder wb) throws AsyncException, InterruptedException, IOException {
    if (SyncLevel.BATCH_ASYNC == syncLevel) {
      dataCubeIo.writeAsync(op, wb);
    } else {
      dataCubeIo.writeSync(op, wb);
    }
  }
}
