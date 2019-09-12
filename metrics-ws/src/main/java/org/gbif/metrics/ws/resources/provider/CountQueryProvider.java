package org.gbif.metrics.ws.resources.provider;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.metrics.es.EsMetricsService;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Singleton;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;


/**
 * A provider that will convert parameters from the request into an INTERNAL cube reader.
 */
@Provider
@Singleton
public class CountQueryProvider implements InjectableProvider<ProvidedCountQuery, Parameter> {


  @Context
  private final HttpContext hc;

  public CountQueryProvider(@Context HttpContext hc) {
    this.hc = hc;
  }

  // The type casting is actually checked at runtime, and there _appears_ to be no way of
  // checking the enum, hence the rawtypes and unchecked warnings suppression.
  @SuppressWarnings({"unchecked", "rawtypes"})
  @VisibleForTesting
  EsMetricsService.CountQuery build(MultivaluedMap<String, String> params) throws ServiceUnavailableException, IllegalArgumentException {
    Set<EsMetricsService.Parameter> parameters = new HashSet<>();
    for (Entry<String, List<String>> param : params.entrySet()) {
      // We only accept 1 value per parameter
      String k = param.getKey();
      String v = param.getValue().iterator().next();

      // callback is a reserved word in the API and invokes the JS callback hack
      // JQuery also inserts _ when using the callback=?
      if ("callback".equalsIgnoreCase(k) || "_".equalsIgnoreCase(k)) {
        continue;
      }

      parameters.add(new EsMetricsService.Parameter(k,v));

    }
    return new EsMetricsService.CountQuery(parameters);
  }

  @Override
  public Injectable<EsMetricsService.CountQuery> getInjectable(ComponentContext ic, ProvidedCountQuery a, Parameter c) {
    return new Injectable<EsMetricsService.CountQuery>() {

      @Override
      public EsMetricsService.CountQuery getValue() {
        return build(hc.getUriInfo().getQueryParameters());
      }
    };
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }
}
