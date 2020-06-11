package org.gbif.metrics.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.metrics.ws.provider.CountQueryArgumentResolver;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer  {

  @Override
  public void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
    argumentResolvers.add(new CountQueryArgumentResolver());
  }

  @Primary
  @Bean
  public ObjectMapper metricsObjectMapper() {
    return JacksonJsonObjectMapperProvider.getObjectMapper();
  }
}
