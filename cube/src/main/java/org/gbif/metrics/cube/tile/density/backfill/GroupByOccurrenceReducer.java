package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.metrics.cube.mapred.OccurrenceWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Groups by the occurrence emitting the counts.
 * Note: We use IntWritable as the value to allow the transient counts to be accumulated.
 */
public class GroupByOccurrenceReducer extends Reducer<OccurrenceWritable, IntWritable, OccurrenceWritable, IntWritable> {

  private final IntWritable i = new IntWritable();

  /**
   * Groups the counts.
   */
  @Override
  protected void reduce(OccurrenceWritable o, Iterable<IntWritable> values, Context context) throws IOException,
    InterruptedException {
    int count = 0;
    for (IntWritable i : values) {
      count += i.get();
    }
    context.setStatus("A species at latitude[" + o.getLatitude() + "], longitude[" + o.getLongitude() + "], year["
      + o.getYear() + "] has count[" + count + "]");
    i.set(count);
    context.write(o, i);
  }
}
