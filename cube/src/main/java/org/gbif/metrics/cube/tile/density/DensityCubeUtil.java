package org.gbif.metrics.cube.tile.density;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.tile.MercatorProjectionUtil;
import org.gbif.metrics.cube.tile.io.TileContentType;

import java.util.Set;

import com.google.common.collect.Sets;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.WriteBuilder;

/**
 * A utility class to encapsulate the logic needed to convert between the denormalized
 * format of occurrences (which have kingdomKey, phylumKey etc) and the mutations that need to happen to the tiles.
 */
public class DensityCubeUtil {

  public static enum Op {
    ADDITION, SUBTRACTION
  }

  /**
   * Utility to either add or subtract one, to the batch for the given record.
   */
  private static void addMutations(Batch<DensityTile> batch, TileContentType type, String key, int x, int y, int z,
    int pixelsPerCluster, Op op, Occurrence o) {
    DensityTile.Builder b = DensityTile.builder(z, x, y, pixelsPerCluster);
    int count = (op == Op.SUBTRACTION) ? -1 : 1;
    b.collect(Layer.inferFrom(o.getBasisOfRecord(), o.getYear()), o.getDecimalLatitude(), o.getDecimalLongitude(),
      count);
    DensityTile tile = b.build();
    WriteBuilder wb =
      new WriteBuilder(DensityCube.INSTANCE).at(DensityCube.ZOOM, z).at(DensityCube.TILE_X, x)
        .at(DensityCube.TILE_Y, y).at(DensityCube.KEY, key).at(DensityCube.TYPE, type);
    batch.putAll(DensityCube.INSTANCE.getWrites(wb, tile));
  }

  /**
   * For the given occurrence, determines the mutations (addresses and operations) that need
   * to be applied.
   * 
   * @param o The denormalized representation
   * @param op That is going to be applied to the cube
   * @return The batch of updates to apply
   */
  public static Batch<DensityTile> cubeMutations(Occurrence o, Op op, int zoom, int pixelsPerCluster) {
    Double latitude = o.getDecimalLatitude();
    Double longitude = o.getDecimalLongitude();
    Batch<DensityTile> batch = new Batch<DensityTile>();

    if (!o.hasSpatialIssue() && MercatorProjectionUtil.isPlottable(latitude, longitude)) {
      Set<Integer> taxa =
        Sets.newHashSet(o.getKingdomKey(), o.getPhylumKey(), o.getClassKey(), o.getOrderKey(), o.getFamilyKey(),
          o.getGenusKey(), o.getSpeciesKey(), o.getTaxonKey());

      // locate the tile
      int tileX = MercatorProjectionUtil.toTileX(o.getDecimalLongitude(), zoom);
      int tileY = MercatorProjectionUtil.toTileY(o.getDecimalLatitude(), zoom);


      for (Integer id : taxa) {
        if (id != null) {
          addMutations(batch, TileContentType.TAXON, String.valueOf(id), tileX, tileY, zoom, pixelsPerCluster, op, o);
        }
      }
      if (o.getPublishingOrgKey() != null) {
        addMutations(batch, TileContentType.PUBLISHER, String.valueOf(o.getPublishingOrgKey()), tileX, tileY, zoom,
          pixelsPerCluster, op, o);
      }
      if (o.getDatasetKey() != null) {
        addMutations(batch, TileContentType.DATASET, String.valueOf(o.getDatasetKey()), tileX, tileY, zoom,
          pixelsPerCluster, op, o);
      }
      if (o.getCountry() != null) {
        addMutations(batch, TileContentType.COUNTRY, o.getCountry().getIso2LetterCode(), tileX, tileY, zoom,
          pixelsPerCluster, op, o);
      }
      if (o.getPublishingCountry() != null) {
        addMutations(batch, TileContentType.PUBLISHING_COUNTRY, o.getPublishingCountry().getIso2LetterCode(), tileX,
          tileY, zoom,
          pixelsPerCluster, op, o);
      }
    }
    return batch;
  }
}
