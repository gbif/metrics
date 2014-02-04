package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.tile.MercatorProjectionUtil;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/**
 * Reads the HBase table, collecting the terms we are interested in as we know there are duplicates
 * at the same location. This is only to reduce the amount of grouping needed in later MR jobs.
 */
public class LocationCollectorMapper extends TableMapper<OccurrenceWritable, IntWritable> {

  private final static IntWritable ONE = new IntWritable(1);

  /**
   * Reads the table, emits the Result keyed on the Lat Lng if it is a plottable record
   */
  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {

    OccurrenceWritable occ = OccurrenceWritable.newInstance(row);
    // Google only goes +/- 85 degrees and we only want maps with no known issues
    if (!OccurrenceWritable.hasSpatialIssue(occ.getIssues()) && MercatorProjectionUtil.isPlottable(occ)) {
      context.getCounter(occ.getBasisOfRecord()).increment(1);
      context.write(occ, ONE);
    }
  }

}
