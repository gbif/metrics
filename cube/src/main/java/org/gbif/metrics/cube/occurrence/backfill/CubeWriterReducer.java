package org.gbif.metrics.cube.occurrence.backfill;

import org.gbif.metrics.cube.HBaseSourcedBackfill;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.urbanairship.datacube.Util;
import com.yammer.metrics.reporting.GangliaReporter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

/**
 * Accumulates the counts and creates table PUTs.
 */
public class CubeWriterReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {  
  private byte[] COL_FAMILY;
  public final static byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY; 
  
  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
    int total = 0;
    for (IntWritable i : values) {
      total += i.get();
    }
    
    // create the same PUT that data cube would make, and emit it for the reducer to write
    Put put = new Put(key.get()).add(COL_FAMILY, QUALIFIER, Util.longToBytes((long) total));
    context.write(key, put);
    context.getCounter("GBIF", "Cube mutations").increment(1);
  }


  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    COL_FAMILY = Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_CF));
    
    // TODO: configurify this
    GangliaReporter.enable(1, TimeUnit.MINUTES, "b5g2.gbif.org", 8649);
  }
}
