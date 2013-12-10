package org.gbif.metrics.cube.index.country.backfill;

import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.index.common.UuidIntMap;
import org.gbif.metrics.cube.index.country.OccurrenceDatasetCountryCube;

import java.io.IOException;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.WriteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A reducer that sums the counts and then writes to the cube.
 */
public class CubeWriterReducer extends Reducer<Key, IntWritable, NullWritable, NullWritable> {

  private HBaseCubes<UuidIntMap> hbaseCube;

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    hbaseCube.close();
  }

  /**
   * Writes to the cube.
   */
  @Override
  protected void reduce(Key key, Iterable<IntWritable> values, Context context) throws IOException {
    int total = 0;
    for (IntWritable i : values) {
      total += i.get();
    }
    context.setStatus("Dataset[" + key.getDatasetKey() + "],  Country[" + key.getCountry() + "], Count[" + total + "]");
    try {
      UuidIntMap c = UuidIntMap.newInstance(ImmutableMap.<UUID, Integer>of(key.getDatasetKey(), total));
      WriteBuilder wb =
        new WriteBuilder(OccurrenceDatasetCountryCube.INSTANCE).at(OccurrenceDatasetCountryCube.COUNTRY,
          key.getCountry());
      hbaseCube.write(c, wb);

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
    hbaseCube =
      HBaseCubes.newCombiningBatchAsync(OccurrenceDatasetCountryCube.INSTANCE, UuidIntMap.DESERIALIZER,
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_BACKFILL_TABLE)),
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_CF)),
        conf, writeBatchSize);
  }
}
