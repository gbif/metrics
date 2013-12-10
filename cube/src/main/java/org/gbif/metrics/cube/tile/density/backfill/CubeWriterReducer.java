package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.metrics.cube.HBaseCubes;
import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.tile.density.DensityCube;
import org.gbif.metrics.cube.tile.density.DensityTile;
import org.gbif.metrics.cube.tile.density.Layer;
import org.gbif.metrics.cube.tile.io.TileKeyWritable;

import java.io.IOException;

import com.urbanairship.datacube.WriteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Collects the tile, and writes to the cube.
 */
public class CubeWriterReducer extends Reducer<TileKeyWritable, OccurrenceWritable, NullWritable, NullWritable> {

  private int pixelsPerCluster = Backfill.DEFAULT_PIXELS_PER_CLUSTER;
  private HBaseCubes<DensityTile> hbaseCube;

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    hbaseCube.close();
  }

  /**
   * Builds the tile, and pushes to the cube.
   */
  @Override
  protected void reduce(TileKeyWritable k, Iterable<OccurrenceWritable> occurrences, Context context)
    throws IOException {
    DensityTile.Builder b = DensityTile.builder(k.getZ(), k.getX(), k.getY(), pixelsPerCluster);
    int count = 0;
    for (OccurrenceWritable o : occurrences) {
      b.collect(Layer.inferFrom(o), o.getLatitude(), o.getLongitude(), o.getCount());
      count++;
    }
    DensityTile t = b.build();

    context.setStatus("Type[" + k.getType() + "],  Key[" + k.getKey() + "], Z[" + k.getZ() + "], X[" + k.getX()
      + "], Y[" + k.getY() + "], Count[" + count + "]");

    try {

      WriteBuilder wb =
        new WriteBuilder(DensityCube.INSTANCE).at(DensityCube.ZOOM, k.getZ()).at(DensityCube.TILE_X, k.getX())
          .at(DensityCube.TILE_Y, k.getY()).at(DensityCube.KEY, k.getKey()).at(DensityCube.TYPE, k.getType());
      hbaseCube.write(t, wb);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    pixelsPerCluster = conf.getInt(Backfill.KEY_PIXELS_PER_CLUSTER, Backfill.DEFAULT_PIXELS_PER_CLUSTER);
    int writeBatchSize = conf.getInt(Backfill.KEY_WRITE_BATCH_SIZE, Backfill.DEFAULT_WRITE_BATCH_SIZE);
    hbaseCube =
      HBaseCubes.newOverwritingBatchAsync(DensityCube.INSTANCE, DensityTile.DESERIALIZER,
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_BACKFILL_TABLE)),
        Bytes.toBytes(conf.get(HBaseSourcedBackfill.KEY_CF)), conf, writeBatchSize);
  }
}
