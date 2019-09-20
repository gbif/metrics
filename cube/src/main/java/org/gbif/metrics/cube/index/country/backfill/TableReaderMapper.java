package org.gbif.metrics.cube.index.country.backfill;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/**
 * Reads the table, grouping the number of occurrence records by a synthetic key of nubKey:dataset.
 */
public class TableReaderMapper extends TableMapper<Key, IntWritable> {

  private static final IntWritable ONE = new IntWritable(1);

  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
    try {
      Occurrence o = OccurrenceBuilder.buildOccurrence(row);
      Preconditions.checkNotNull(o, "Unable to generate an Occurrence from the HBase using the OccurrenceBuilder");
      if (o.getDatasetKey() != null && o.getCountry() != null) {
        Key k = new Key(o.getDatasetKey(), o.getCountry());
        context.setStatus(k.toString());
        context.write(k, ONE);
      } else {
        context.getCounter("GBIF", "Missing dataset key or country (skipped record)").increment(1);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
