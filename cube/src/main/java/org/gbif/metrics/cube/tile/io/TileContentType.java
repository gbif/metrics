package org.gbif.metrics.cube.tile.io;

/**
 * For indicating the source of content for a tile.
 * Note that we use the terminology PUBLISHING_COUNTRY whereas the Occurrence projects use PUBLISHING_COUNTRY.
 * This is because it really is the publishing country and not the hosting country and it is envisaged that
 * the occurrence projects will be renamed.
 * Be aware that calling hashCode() on an ENUM is not consistent across JVMs!
 */
public enum TileContentType {
  ALL(0), TAXON(1), DATASET(2), PUBLISHER(3), COUNTRY(4), NETWORK(5), PUBLISHING_COUNTRY(6);

  private int id;

  TileContentType(int id) {
    this.id = id;
  }

  public static TileContentType INSTANCE(int id) {
    switch (id) {
      case 1:
        return TAXON;
      case 2:
        return DATASET;
      case 3:
        return PUBLISHER;
      case 4:
        return COUNTRY;
      case 5:
        return NETWORK;
      case 6:
        return PUBLISHING_COUNTRY;
      default:
        return ALL;
    }
  }

  public int getId() {
    return id;
  }
}
