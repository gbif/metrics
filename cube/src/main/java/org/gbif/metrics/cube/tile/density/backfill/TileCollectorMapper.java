package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.tile.MercatorProjectionUtil;
import org.gbif.metrics.cube.tile.io.TileContentType;
import org.gbif.metrics.cube.tile.io.TileKeyWritable;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads the HBase table, collecting the points by tile.
 */
public class TileCollectorMapper extends Mapper<OccurrenceWritable, IntWritable, TileKeyWritable, OccurrenceWritable> {

  private int numberZooms;


  @Override
  protected void map(OccurrenceWritable o, IntWritable count, Context context) throws IOException, InterruptedException {
    context.setStatus("Latitude[" + o.getLatitude() + "], Longitude[" + o.getLongitude() + "], issues[" + o.getIssues() + "] has count[" + o.getCount() + "]");
    o.setCount(count.get()); // cannot be set earlier, since we need to group at the occurrence

    // Google only goes +/- 85 degrees and we only want maps with no known issues
    if (MercatorProjectionUtil.isPlottable(o.getLatitude(), o.getLongitude()) && !OccurrenceWritable.hasSpatialIssue(o.getIssues())) {
      Set<Integer> taxa =
        Sets.newHashSet(o.getKingdomID(), o.getPhylumID(), o.getClassID(), o.getOrderID(), o.getFamilyID(),
                        o.getGenusID(), o.getSubgenusID(), o.getSpeciesID(), o.getTaxonID());

      for (int z = 0; z < numberZooms; z++) {
        context.setStatus("Lat[" + o.getLatitude() + "] lng[" + o.getLongitude() + "] zoom[" + z + " of " + numberZooms + "]");
        // locate the tile
        int tileX = MercatorProjectionUtil.toTileX(o.getLongitude(), z);
        int tileY = MercatorProjectionUtil.toTileY(o.getLatitude(), z);

        for (Integer id : taxa) {
          if (id != null) {
            context.write(new TileKeyWritable(TileContentType.TAXON, String.valueOf(id), tileX, tileY, z), o);
          }
        }
        if (o.getPublishingOrganisationKey() != null) {
          context.write(new TileKeyWritable(TileContentType.PUBLISHER, o.getPublishingOrganisationKey(), tileX, tileY, z), o);
        }
        if (o.getDatasetKey() != null) {
          context.write(new TileKeyWritable(TileContentType.DATASET, o.getDatasetKey(), tileX, tileY, z), o);
        }
        if (o.getCountry() != null) {
          context.write(new TileKeyWritable(TileContentType.COUNTRY, o.getCountry(), tileX, tileY, z), o);
        }
        if (o.getPublishingCountry() != null) {
          context.write(new TileKeyWritable(TileContentType.PUBLISHING_COUNTRY, o.getPublishingCountry(), tileX, tileY, z), o);
        }
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    numberZooms = context.getConfiguration().getInt(Backfill.KEY_NUM_ZOOMS, Backfill.DEFAULT_NUM_ZOOMS);
  }
}
