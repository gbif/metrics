package org.gbif.metrics.cube.index.common;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sums the INTs and emits by the same key.
 * This is intended as a combiner, to improve the efficiency of the total operation.
 */
public class Combiner<T> extends Reducer<WritableComparable<T>, IntWritable, WritableComparable<T>, IntWritable> {

  private final IntWritable i = new IntWritable();

  @Override
  protected void reduce(WritableComparable<T> key, Iterable<IntWritable> value, Context context) throws IOException,
    InterruptedException {
    int total = 0;
    for (IntWritable i : value) {
      total += i.get();
    }
    context.setStatus("Key[" + key.toString() + "], Count[" + total + "]");
    i.set(total);
    context.write(key, i);
  }
}
