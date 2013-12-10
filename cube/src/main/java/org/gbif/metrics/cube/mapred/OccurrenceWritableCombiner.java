package org.gbif.metrics.cube.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Accumulates the counts of occurrences, but DOES NOT set the count on the occurrence itself.
 * This is required so that it can have several passes, and the keys remain the same.
 */
public class OccurrenceWritableCombiner extends Reducer<OccurrenceWritable, IntWritable, OccurrenceWritable, IntWritable> {

  private final IntWritable i = new IntWritable();

  @Override
  protected void reduce(OccurrenceWritable key, Iterable<IntWritable> value, Context context) throws IOException,
    InterruptedException {
    int total = 0;
    for (IntWritable i : value) {
      total += i.get();
    }
    context.setStatus("Occurrence from dataset[" + key.getDatasetKey() + "],  Taxon[" + key.getTaxonID() + "], Count["
      + total + "]");
    i.set(total);
    context.write(key, i);
  }
}
