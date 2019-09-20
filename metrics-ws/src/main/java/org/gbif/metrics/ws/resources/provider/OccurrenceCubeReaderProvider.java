package org.gbif.metrics.ws.resources.provider;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.vocabulary.Country;
import org.gbif.metrics.cube.occurrence.OccurrenceCube;

import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;
import com.urbanairship.datacube.ReadBuilder;


/**
 * A provider that will convert parameters from the request into an INTERNAL cube reader.
 */
@Provider
@Singleton
public class OccurrenceCubeReaderProvider implements InjectableProvider<ProvidedOccurrenceCubeReader, Parameter> {

  // Provides an index to lookup the API Dimension by the HTTP parameter key
  private static final ImmutableMap<String, Dimension<?>> PARAM_INDEX;

  static {
    ImmutableMap.Builder<String, Dimension<?>> b = new ImmutableMap.Builder<String, Dimension<?>>();
    for (Dimension<?> d : org.gbif.api.model.metrics.cube.OccurrenceCube.DIMENSIONS) {
      b.put(d.getKey(), d);
    }
    PARAM_INDEX = b.build();
  }

  @Context
  private final HttpContext hc;

  public OccurrenceCubeReaderProvider(@Context HttpContext hc) {
    this.hc = hc;
  }

  // The type casting is actually checked at runtime, and there _appears_ to be no way of
  // checking the enum, hence the rawtypes and unchecked warnings suppression.
  @SuppressWarnings({"unchecked", "rawtypes"})
  @VisibleForTesting
  ReadBuilder build(MultivaluedMap<String, String> params) throws ServiceUnavailableException, IllegalArgumentException {
    ReadBuilder b = new ReadBuilder(OccurrenceCube.INSTANCE);
    for (Entry<String, List<String>> param : params.entrySet()) {
      // We only accept 1 value per parameter
      String k = param.getKey();
      String v = param.getValue().iterator().next();

      // callback is a reserved word in the API and invokes the JS callback hack
      // JQuery also inserts _ when using the callback=?
      if ("callback".equalsIgnoreCase(k) || "_".equalsIgnoreCase(k)) {
        continue;
      }

      Dimension<?> dim = PARAM_INDEX.get(k);
      Preconditions.checkArgument(dim != null, "Unknown parameter found in the request, does not map to a dimension: "
        + k);
      Class<?> type = dim.getType();

      // translate the API dimension to the internal dimension, typing the value
      // Note: The API_MAPPING might be determined at runtime, with the cube's implementing an interface allowing that
      // to be read.
      com.urbanairship.datacube.Dimension<?> internalDim = OccurrenceCube.API_MAPPING.get(dim);
      if (internalDim == null) {
        throw new ServiceUnavailableException("Unable to convert address in the cube.  No dimension exists for "
          + dim.getKey());
      } else if (String.class == type) {
        b.at(internalDim, v);

      } else if (Integer.class == type) {
        b.at(internalDim, Integer.valueOf(v));

      } else if (Float.class == type) {
        b.at(internalDim, Float.valueOf(v));

      } else if (Double.class == type) {
        b.at(internalDim, Double.valueOf(v));

      } else if (Boolean.class == type) {
        b.at(internalDim, Boolean.valueOf(v));

      } else if (UUID.class == type) {
        b.at(internalDim, UUID.fromString(v));

      } else if (Country.class.isAssignableFrom(type)) {
        b.at(internalDim, Country.fromIsoCode(v));

      } else if (Enum.class.isAssignableFrom(type)) {
        b.at(internalDim, Enum.valueOf((Class<Enum>) dim.getType(), v));

      } else {
        throw new ServiceUnavailableException("Dimension is of unknown type: " + type);
      }
    }
    return b;
  }

  @Override
  public Injectable<ReadBuilder> getInjectable(ComponentContext ic, ProvidedOccurrenceCubeReader a, Parameter c) {
    return new Injectable<ReadBuilder>() {

      @Override
      public ReadBuilder getValue() {
        return build(hc.getUriInfo().getQueryParameters());
      }
    };
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }
}
