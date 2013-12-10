package org.gbif.metrics.ws.guice;

import org.gbif.metrics.cube.index.country.guice.CountryOccurrenceDatasetHBaseModule;
import org.gbif.metrics.cube.index.taxon.guice.TaxonOccurrenceDatasetHBaseModule;
import org.gbif.metrics.cube.occurrence.guice.OccurrenceCubeHBaseModule;
import org.gbif.ws.server.guice.GbifServletListener;

import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.inject.Module;


/**
 * The Registry WS production module.
 */
public class MetricsWsServletListener extends GbifServletListener {

  public static final String APPLICATION_PROPERTIES = "metrics.properties";

  public MetricsWsServletListener() {
    super(APPLICATION_PROPERTIES, "org.gbif.metrics.ws", false);

  }

  @Override
  protected List<Module> getModules(Properties props) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new OccurrenceCubeHBaseModule(props));
    modules.add(new TaxonOccurrenceDatasetHBaseModule(props));
    modules.add(new CountryOccurrenceDatasetHBaseModule(props));
    return modules;
  }
}
