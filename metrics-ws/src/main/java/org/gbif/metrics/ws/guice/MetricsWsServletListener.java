package org.gbif.metrics.ws.guice;

import org.gbif.metrics.cube.index.country.guice.CountryOccurrenceDatasetHBaseModule;
import org.gbif.metrics.cube.index.taxon.guice.TaxonOccurrenceDatasetHBaseModule;
import org.gbif.metrics.cube.occurrence.guice.OccurrenceCubeHBaseModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import org.gbif.ws.server.guice.GbifServletListener;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.inject.Module;


/**
 * The Registry WS production module.
 */
public class MetricsWsServletListener extends GbifServletListener {

  private static final String METRICS_CONF = "metrics.properties";

  public MetricsWsServletListener() throws IOException {
    super(PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(METRICS_CONF)), "org.gbif.metrics.ws", false);
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
