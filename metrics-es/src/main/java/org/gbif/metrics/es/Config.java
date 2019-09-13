package org.gbif.metrics.es;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Metrics service configuration.
 */
public class Config {

  private final long expireCacheAfter;

  private final List<URL> hosts;

  private final String indexName;

  private Config(String indexName, String[] hostsAddresses, long expireCacheAfter) {
    this.expireCacheAfter = expireCacheAfter;
    this.indexName = indexName;
    hosts =
        Arrays.stream(hostsAddresses)
            .map(
                address -> {
                  try {
                    return new URL(address);
                  } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(address + " is not a valid url", e);
                  }
                })
            .collect(Collectors.toList());
  }

  /**
   * Creates a {@link Config} from the addresses received.
   *
   * @param expireCacheAfter time to expire cache entries
   * @param indexName Elasticsearch index/alias name
   * @param hostsAddresses they should be valid URLs.
   * @return {@link Config}
   */
  public static Config from(long expireCacheAfter, String indexName, String... hostsAddresses) {
    return new Config(indexName, hostsAddresses, expireCacheAfter);
  }

  /**
   * @return time to expire cache entries
   */
  public long getExpireCacheAfter() {
    return expireCacheAfter;
  }

  /**
   * @return list of Elasticsearch hosts
   */
  public List<URL> getHosts() {
    return hosts;
  }

  /**
   * @return Elasticsearch index/alias name
   */
  public String getIndexName() {
    return indexName;
  }
}
