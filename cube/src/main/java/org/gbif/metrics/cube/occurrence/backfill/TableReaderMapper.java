package org.gbif.metrics.cube.occurrence.backfill;

import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.occurrence.OccurrenceAddressUtil;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;

import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/**
 * Reads the table, emitting the cube address to be updated and ONE for the count.
 */
public class TableReaderMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

  private final IntWritable ONE = new IntWritable(1);
  private final ImmutableBytesWritable KEY = new ImmutableBytesWritable();
  private static final byte[] CUBE_NAME = ArrayUtils.EMPTY_BYTE_ARRAY;


  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {

    OccurrenceWritable o = new OccurrenceWritable(OccurrenceBuilder.buildOccurrence(row), 1);

    // determine the address
    Batch<LongOp> addresses = OccurrenceAddressUtil.cubeMutation(o, new LongOp(1));
    for (Address a : addresses.getMap().keySet()) {
      byte[] rowKey = ArrayUtils.addAll(CUBE_NAME, a.toKey(null));
      KEY.set(rowKey);
      context.write(KEY, ONE);
    }
    context.setStatus("Occurrence from dataset[" + o.getDatasetKey() + "] produced [" + addresses.getMap().size() + "] cube mutations");
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
  }
}
