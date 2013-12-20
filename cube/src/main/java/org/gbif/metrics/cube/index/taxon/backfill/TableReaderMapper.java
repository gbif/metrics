package org.gbif.metrics.cube.index.taxon.backfill;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrencestore.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/**
 * Reads the table, grouping the number of occurrence records by a synthetic key of nubKey:dataset.
 */
public class TableReaderMapper extends TableMapper<Key, IntWritable> {

  private static final IntWritable ONE = new IntWritable(1);

  private void insertIfPresent(Set<Integer> t, Integer v) {
    if (v != null) {
      t.add(v);
    }
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
    try {
      Occurrence o = OccurrenceBuilder.buildOccurrence(row);
      Preconditions.checkNotNull(o, "Unable to generate an Occurrence from the HBase using the OccurrenceBuilder");

      if (o.getDatasetKey() != null) {
        Set<Integer> nubs = Sets.newHashSet();
        insertIfPresent(nubs, o.getKingdomKey());
        insertIfPresent(nubs, o.getPhylumKey());
        insertIfPresent(nubs, o.getClassKey());
        insertIfPresent(nubs, o.getOrderKey());
        insertIfPresent(nubs, o.getFamilyKey());
        insertIfPresent(nubs, o.getGenusKey());
        insertIfPresent(nubs, o.getSubgenusKey());
        insertIfPresent(nubs, o.getSpeciesKey());
        insertIfPresent(nubs, o.getTaxonKey());
        for (Integer i : nubs) {
          Key k = new Key(o.getDatasetKey(), i);
          context.setStatus(k.toString());
          context.write(k, ONE);
        }
      } else {
        context.getCounter("GBIF", "Missing dataset key (skipped record)").increment(1);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
