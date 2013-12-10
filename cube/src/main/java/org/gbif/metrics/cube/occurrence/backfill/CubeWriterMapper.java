package org.gbif.metrics.cube.occurrence.backfill;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.occurrence.OccurrenceAddressUtil;
import org.gbif.metrics.cube.occurrence.OccurrenceCube;
import org.gbif.occurrencestore.persistence.util.OccurrenceBuilder;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

/**
 * @deprecated In favor of {@link TableReaderMapper} and {@link CubeWriteReducer}
 */
@Deprecated
public class CubeWriterMapper extends TableMapper<NullWritable, NullWritable> {

  private HBaseCubes<LongOp> cube;

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    cube.close();
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
    try {
      Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);
      Preconditions.checkNotNull(occurrence,
        "Unable to generate an Occurrence from the HBase using the OccurrenceBuilder");
      Batch<LongOp> update = OccurrenceAddressUtil.cubeMutation(occurrence, new LongOp(1));

      context.setStatus("Handling occurrence[" + occurrence.getKey() + "] produced " + update.getMap().size()
        + " mutations");

      context.getCounter("GBIF", "Cube mutations").increment(update.getMap().size());

      // Note: the cube is a batch async cube to allow this (see setup())
      cube.write(update);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    int writeBatchSize =
      conf.getInt(HBaseSourcedBackfill.KEY_WRITE_BATCH_SIZE, HBaseSourcedBackfill.DEFAULT_WRITE_BATCH_SIZE);
    // this is the basic incrementing as each record increments the existing count per rollup
    // NOTE: Very importantly this is in BatchAsync to enable the writing in batches
    cube =
      HBaseCubes.newIncrementingBatchAsync(OccurrenceCube.INSTANCE, LongOp.DESERIALIZER,
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_BACKFILL_TABLE)),
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_CF)),
        conf, writeBatchSize);
  }
}
