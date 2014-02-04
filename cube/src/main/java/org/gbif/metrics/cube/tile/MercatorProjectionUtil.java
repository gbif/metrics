package org.gbif.metrics.cube.tile;

import org.gbif.metrics.cube.mapred.OccurrenceWritable;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

/**
 * Utilities for dealing with Google tiles.
 */
public class MercatorProjectionUtil {

  public static final int TILE_SIZE = 256;

  /**
   * @return the X pixel offset within the tile
   */
  public static int getOffsetX(double lat, double lng, int zoom) {
    double x = toNormalisedPixelCoords(lat, lng).getX();
    int scale = 1 << zoom;
    x *= scale * TILE_SIZE;
    return (int) (x % TILE_SIZE);
  }


  /**
   * @return the Y pixel offset within the tile.
   */
  public static int getOffsetY(double lat, double lng, int zoom) {
    double y = toNormalisedPixelCoords(lat, lng).getY();
    int scale = 1 << zoom;
    y *= scale * TILE_SIZE;
    int tileY = toTileY(lat, zoom);
    return (int) (y - (TILE_SIZE * tileY));
  }

  /**
   * @return a Rectangle2D with x = lon, y = lat, width=lonSpan, height=latSpan
   *         for an x,y,zoom as used by google.
   */
  public static Rectangle2D.Double getTileRect(int x, int y, int zoom) {
    int tilesAtThisZoom = 1 << (17 - zoom);
    double lngWidth = 360.0 / tilesAtThisZoom; // width in degrees longitude
    double lng = -180 + (x * lngWidth); // left edge in degrees longitude

    double latHeightMerc = 1.0 / tilesAtThisZoom; // height in "normalized" mercator 0,0 top left
    double topLatMerc = y * latHeightMerc; // top edge in "normalized" mercator 0,0 top left
    double bottomLatMerc = topLatMerc + latHeightMerc;

    // convert top and bottom lat in mercator to degrees
    // note that in fact the coordinates go from about -85 to +85 not -90 to 90!
    double bottomLat = Math.toDegrees((2 * Math.atan(Math.exp(Math.PI * (1 - (2 * bottomLatMerc))))) - (Math.PI / 2));

    double topLat = Math.toDegrees((2 * Math.atan(Math.exp(Math.PI * (1 - (2 * topLatMerc))))) - (Math.PI / 2));

    double latHeight = topLat - bottomLat;

    return new Rectangle2D.Double(lng, bottomLat, lngWidth, latHeight);
  }

  /**
   * Google maps cover +/- 85 degrees only.
   *
   * @return true if the location is plottable on a map
   */
  public static boolean isPlottable(Double lat, Double lng) {
    return (lat != null && lng != null && lat >= -85d && lat <= 85d && lng >= -180 && lng <= 180);
  }

  public static boolean isPlottable(OccurrenceWritable occ) {
    return isPlottable(occ.getLatitude(), occ.getLongitude());
  }

  /**
   * Returns the lat/lng as an "Offset Normalized Mercator" pixel coordinate,
   * this is a coordinate that runs from 0..1 in latitude and longitude with 0,0 being
   * top left. Normalizing means that this routine can be used at any zoom level and
   * then multiplied by a power of two to get actual pixel coordinates.
   */
  public static Point2D toNormalisedPixelCoords(double lat, double lng) {
    // first convert to Mercator projection
    // first convert the lat lon to mercator coordintes.
    if (lng > 180) {
      lng -= 360;
    }

    lng /= 360;
    lng += 0.5;

    lat = 0.5 - ((Math.log(Math.tan((Math.PI / 4) + ((0.5 * Math.PI * lat) / 180))) / Math.PI) / 2.0);

    return new Point2D.Double(lng, lat);
  }

  /**
   * @return The tile index for the longitude at the given zoom
   */
  public static int toTileX(double lng, int zoom) {
    if (lng > 180) {
      lng -= 360;
    }

    lng /= 360;
    lng += 0.5;
    int scale = 1 << zoom;
    return (int) (lng * scale);
  }


  /**
   * Returns a point that is a google tile reference for the tile containing
   * the lat/lng and at the zoom level.
   */
  public static Point toTileXY(double lat, double lng, int zoom) {
    Point2D normalised = toNormalisedPixelCoords(lat, lng);
    int scale = 1 << zoom;

    // can just truncate to integer, this looses the fractional "pixel offset"
    return new Point((int) (normalised.getX() * scale), (int) (normalised.getY() * scale));
  }

  /**
   * @return The tile index for the latitude at the given zoom
   */
  public static int toTileY(double lat, int zoom) {
    lat = 0.5 - ((Math.log(Math.tan((Math.PI / 4) + ((0.5 * Math.PI * lat) / 180))) / Math.PI) / 2.0);
    int scale = 1 << zoom;
    return (int) (lat * scale);
  }
}
