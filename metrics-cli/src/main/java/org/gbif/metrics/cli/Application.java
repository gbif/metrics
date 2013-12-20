package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.metrics.cube.index.country.OccurrenceDatasetCountryCube;
import org.gbif.metrics.cube.index.taxon.OccurrenceDatasetCube;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Service;
import org.kohsuke.MetaInfServices;

/**
 * A container of commands that are annotated such that they will be picked up by the CLI framework and expose the
 * services that can be run.
 */
public class Application {

  @MetaInfServices(Command.class)
  public static class DensityCubeCommand extends ServiceCommand {

    private final DensityCubeConfiguration configuration = new DensityCubeConfiguration();

    public DensityCubeCommand() {
      super("DensityCube");
    }

    @Override
    protected Object getConfigurationObject() {
      return configuration;
    }

    @Override
    protected Service getService() {
      return new DensityCubeUpdaterService(configuration);
    }
  }

  @MetaInfServices(Command.class)
  public static class OccurrenceCubeCommand extends ServiceCommand {

    private final CubeConfiguration configuration = new CubeConfiguration();

    public OccurrenceCubeCommand() {
      super("OccurrenceCube");
    }

    @Override
    protected Object getConfigurationObject() {
      return configuration;
    }

    @Override
    protected Service getService() {
      return new OccurrenceCubeUpdaterService(configuration);
    }
  }


  @MetaInfServices(Command.class)
  public static class OccurrenceDatasetCountryCubeCommand extends ServiceCommand {

    private final CubeConfiguration configuration = new CubeConfiguration();

    public OccurrenceDatasetCountryCubeCommand() {
      super("OccurrenceDatasetCountryCube");
    }

    @Override
    protected Object getConfigurationObject() {
      return configuration;
    }

    @Override
    protected Service getService() {
      return new OccurrenceUuidIntMapCubeUpdaterService<Country>(configuration, OccurrenceDatasetCountryCube.INSTANCE,
        OccurrenceDatasetCountryCube.COUNTRY, countryGetter());
    }

    /**
     * Function that return the country value from an occurrence object.
     */
    private Function<Occurrence, Country> countryGetter() {
      return new Function<Occurrence, Country>() {

        @Override
        public Country apply(Occurrence occurrence) {
          return occurrence.getCountry();
        }

      };
    }
  }


  @MetaInfServices(Command.class)
  public static class OccurrenceDatasetTaxonCubeCommand extends ServiceCommand {

    private final CubeConfiguration configuration = new CubeConfiguration();

    public OccurrenceDatasetTaxonCubeCommand() {
      super("OccurrenceDatasetTaxonCube");
    }

    @Override
    protected Object getConfigurationObject() {
      return configuration;
    }

    @Override
    protected Service getService() {
      return new OccurrenceUuidIntMapCubeUpdaterService<Integer>(configuration, OccurrenceDatasetCube.INSTANCE,
        OccurrenceDatasetCube.NUB_KEY, nubKeyGetter());
    }

    /**
     * Function that return the nubKey value from an occurrence object.
     */
    private Function<Occurrence, Integer> nubKeyGetter() {
      return new Function<Occurrence, Integer>() {

        @Override
        public Integer apply(Occurrence occurrence) {
          return occurrence.getTaxonKey();
        }

      };
    }
  }
}
