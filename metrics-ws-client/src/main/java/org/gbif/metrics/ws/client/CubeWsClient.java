package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.service.metrics.CubeService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * Client-side implementation to the generic cube service.
 */
public interface CubeWsClient extends CubeService {

  @Override
  default long get(ReadBuilder addressBuilder) throws IllegalArgumentException {
    Preconditions.checkNotNull(addressBuilder, "The cube address is mandatory");
    Map<String, String> params = new HashMap<>();
    for (Entry<Dimension<?>, String> d : addressBuilder.build().entrySet()) {
      params.put(d.getKey().getKey(), d.getValue());
    }
    return count(params);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/count",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Long count(@SpringQueryMap Map<String, String> params);

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/count/schema",
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Override
  List<Rollup> getSchema();
}
