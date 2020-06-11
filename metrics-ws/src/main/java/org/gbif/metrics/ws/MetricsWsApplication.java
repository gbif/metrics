package org.gbif.metrics.ws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(
    basePackages = {
        "org.gbif.ws.server.advice",
        "org.gbif.ws.server.mapper",
        "org.gbif.metrics.ws"
    })
public class MetricsWsApplication {

  public static void main(String[] args) {
    SpringApplication.run(MetricsWsApplication.class, args);
  }
}
