package org.gbif.metrics.cli;

import org.gbif.metrics.cube.tile.density.DensityTile;

import org.apache.hadoop.conf.Configuration;

/**
 * The service to update density cubes.
 */
class DensityCubeUpdaterService extends CubeUpdaterService<DensityCubeConfiguration> {

  // by design, we batch and then flush in a singleton instance to avoid competition on tile updates
  private DensityCubeCallback callback;

  DensityCubeUpdaterService(DensityCubeConfiguration configuration) {
    super(configuration);
  }

  @Override
  CubeUpdaterCallback<DensityTile> getCallback(final DensityCubeConfiguration configuration,
    final Configuration hadoopConfiguration) {
    callback = new DensityCubeCallback(configuration, hadoopConfiguration);
    return callback;
  }

  @Override
  protected void shutDown() throws Exception {
    if (callback != null) {
      callback.close();
    }
  }
}
