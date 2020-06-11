package org.gbif.metrics.ws.provider;

import org.gbif.metrics.es.CountQuery;
import org.gbif.metrics.es.Parameter;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A provider that will convert parameters from the request into an INTERNAL cube reader.
 */
@SuppressWarnings("NullableProblems")
public class CountQueryArgumentResolver implements HandlerMethodArgumentResolver {

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    return parameter.getParameterAnnotation(ProvidedCountQuery.class) != null;
  }

  @Override
  public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest webRequest, WebDataBinderFactory binderFactory) throws Exception {
    Set<Parameter> parameters = new HashSet<>();
    for (Map.Entry<String, String[]> param : webRequest.getParameterMap().entrySet()) {
      // We only accept 1 value per parameter
      String k = param.getKey();
      String v = param.getValue()[0];

      // callback is a reserved word in the API and invokes the JS callback hack
      // JQuery also inserts _ when using the callback=?
      if ("callback".equalsIgnoreCase(k) || "_".equalsIgnoreCase(k)) {
        continue;
      }

      parameters.add(new Parameter(k, v));

    }
    return new CountQuery(parameters);
  }
}
