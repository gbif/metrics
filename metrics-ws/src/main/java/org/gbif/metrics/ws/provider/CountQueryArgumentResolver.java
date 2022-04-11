/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.metrics.ws.provider;

import org.gbif.metrics.es.CountQuery;
import org.gbif.metrics.es.Parameter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/** A provider that will convert parameters from the request into an INTERNAL cube reader. */
@SuppressWarnings("NullableProblems")
public class CountQueryArgumentResolver implements HandlerMethodArgumentResolver {

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    return parameter.getParameterAnnotation(ProvidedCountQuery.class) != null;
  }

  @Override
  public Object resolveArgument(
      MethodParameter parameter,
      ModelAndViewContainer mavContainer,
      NativeWebRequest webRequest,
      WebDataBinderFactory binderFactory) {
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
