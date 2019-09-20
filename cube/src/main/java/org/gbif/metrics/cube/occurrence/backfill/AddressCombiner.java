package org.gbif.metrics.cube.occurrence.backfill;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Accumulates the counts and creates table PUTs.
 */
public class AddressCombiner extends Reducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, IntWritable> {  
  private final IntWritable COUNT = new IntWritable(1);
  
  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
    int total = 0;
    for (IntWritable i : values) {
      total += i.get();
    }
    COUNT.set(total);
    
    context.write(key, COUNT);
    context.getCounter("GBIF", "Combined count").increment(total);
  }
}
