package org.gbif.metrics.cli;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

/**
 * Extends the cube configuration with density cube specific parameters to control the granularity (e.g. how big the
 * clusters are) and the number of zoom levels to which the tiles are precalculated.
 */
class DensityCubeConfiguration extends CubeConfiguration {

  @Parameter(names = "--pixels-per-cluster",
    description = "Controls the granularity of the density map.  Should the default be changed, any readers of the "
      + "cube (e.g. tile-server) need to be reconfigured - use with caution!")
  @Min(1)
  @Max(128)
  public int pixelsPerCluster = 1;

  @Parameter(names = "--zoom", description = "Controls the zoom level to process (0-18)")
  @Min(0)
  @Max(18)
  public int zoom = 18;


  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString()).add("pixelsPerCluster", pixelsPerCluster)
      .add("zoom", zoom).toString();
  }
}
