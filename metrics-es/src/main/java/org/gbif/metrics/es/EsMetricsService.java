package org.gbif.metrics.es;

import org.gbif.metrics.MetricsService;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.http.HttpHost;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricsSevice based on Elasticsearch.
 */
public class EsMetricsService implements MetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(EsMetricsService.class);

  private static final int AGG_SIZE = 30_000;
  private static final int SHARD_SIZE = 10_000;

  //Map of dimensions/parameter.name to Elasticsearch fields
  private static final Map<String,String> DIMENSION_TO_ES_FIELD;
  static {
    Map<String,String> fieldsMap = new HashMap<>();
    fieldsMap.put("basisOfRecord", "basisOfRecord");
    fieldsMap.put("country", "countryCode");
    fieldsMap.put("isGeoreferenced", "hasCoordinate");
    fieldsMap.put("taxonKey", "gbifClassification.taxonKey");
    fieldsMap.put("datasetKey", "datasetKey");
    fieldsMap.put("publishingCountry", "datasetPublishingCountry");
    fieldsMap.put("typeStatus", "typeStatus");
    fieldsMap.put("issue", "issues");
    fieldsMap.put("year", "year");
    fieldsMap.put("kingdom", "gbifClassification.kingdom.keyword");
    DIMENSION_TO_ES_FIELD = Collections.unmodifiableMap(fieldsMap);
  }

  //Cache for count queries
  private final Cache<CountQuery, Long> countCache;

  //Cache for aggregation queries
  private final Cache<AggregationQuery, Map<String,Long>> aggregationsCache;

  private final Config config;

  private final RestHighLevelClient restClient;

  public EsMetricsService(Config config) {
    this.config = config;
    restClient = buildClient(config);
    countCache = new Cache2kBuilder<CountQuery,Long>(){}
      .loader(new CacheLoader<CountQuery, Long>() {
        @Override
        public Long load(final CountQuery key) throws Exception {
          return loadCount(key);
        }
      })
      .expireAfterWrite(config.getExpireCacheAfter(), TimeUnit.MILLISECONDS)
      .build();

    aggregationsCache = new Cache2kBuilder<AggregationQuery, Map<String,Long>>(){}
      .loader(new CacheLoader<AggregationQuery, Map<String,Long>>() {
        @Override
        public Map<String,Long> load(final AggregationQuery key) throws Exception {
          return loadAggregation(key);
        }
      })
      .expireAfterWrite(config.getExpireCacheAfter(), TimeUnit.MILLISECONDS)
      .build();
  }

  /**
   * Loader function for the count queries cache.
   */
  private Long loadCount(CountQuery countQuery) {
    try {
      return restClient.count(buildCountRequest(countQuery), RequestOptions.DEFAULT).getCount();
    } catch (IOException ex) {
      LOG.error("Error executing CountQuery {}", countQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Loader function for the aggregation queries cache.
   */
  private Map<String,Long> loadAggregation(AggregationQuery aggregationQuery) {
    try {
      SearchResponse response = restClient.search(buildCountsAggregateRequest(aggregationQuery), RequestOptions.DEFAULT);
      List<? extends Terms.Bucket> buckets = ((Terms)response.getAggregations().get(aggregationQuery.getDimension())).getBuckets();
      Map<String,Long> aggregation = new LinkedHashMap<>(buckets.size());
      //Results added in reverse order because the ES API returns them like that
      for(int i = buckets.size() - 1; i >= 0; i--) {
        Terms.Bucket bucket = buckets.get(i);
        aggregation.put(aggregationQuery.getKeyLabelTransform().apply(bucket.getKeyAsString()), bucket.getDocCount());
      }
      return aggregation;
    } catch (IOException ex) {
      LOG.error("Error executing AggregationQuery {}", aggregationQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Builds an Elasticsearch {@link CountRequest} from a {@link CountQuery}.
   */
  private CountRequest buildCountRequest(CountQuery countQuery) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    countQuery.getParameters().forEach(p -> bool.filter().add(buildQuery(p)));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(bool);
    CountRequest countRequest = new CountRequest();
    countRequest.source(searchSourceBuilder);
    countRequest.indices(config.getIndexName());
    return countRequest;
  }

  /**
   * Builds a {@link SearchRequest} with the aggregation parameters from a {@link AggregationQuery}.
   */
  private SearchRequest buildCountsAggregateRequest(AggregationQuery aggregationQuery) {
    TermsAggregationBuilder aggregation = AggregationBuilders.terms(aggregationQuery.getDimension())
                                                              .order(BucketOrder.count(true)) //Order by count
                                                              .field(DIMENSION_TO_ES_FIELD.get(aggregationQuery.getDimension()))
                                                              .size(AGG_SIZE)
                                                              .shardSize(SHARD_SIZE);
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    aggregationQuery.getParameters().forEach(parameter -> boolQueryBuilder.filter(buildQuery(parameter)));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder.filter().isEmpty()? QueryBuilders.matchAllQuery() : boolQueryBuilder);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(aggregation);
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(config.getIndexName());
    return searchRequest;
  }

  /**
   * Simple query builders for ranges and terms.
   */
  private QueryBuilder buildQuery(Parameter parameter) {
    if (parameter.getValue().contains(",")) {
      String[] values = parameter.getValue().split(",");
      return QueryBuilders.rangeQuery(DIMENSION_TO_ES_FIELD.get(parameter.getName())).gte(values[0]).lte(values[1]);
    }
    return QueryBuilders.termQuery(DIMENSION_TO_ES_FIELD.get(parameter.getName()), parameter.getValue());
  }

  /**
   * Elasticsearch client builder.
   */
  private static RestHighLevelClient buildClient(Config config) {
    Objects.requireNonNull(config);
    Objects.requireNonNull(config.getHosts());
    Preconditions.checkArgument(!config.getHosts().isEmpty());

    HttpHost[] hosts = new HttpHost[config.getHosts().size()];
    for (int i = 0; i < config.getHosts().size(); i++) {
      URL urlHost = config.getHosts().get(i);
      hosts[i] = new HttpHost(urlHost.getHost(), urlHost.getPort(), urlHost.getProtocol());
    }

    return new RestHighLevelClient(RestClient.builder(hosts).setMaxRetryTimeoutMillis(180_000));
  }

  @Override
  public Long count(CountQuery countQuery) {
    return  countCache.get(countQuery);
  }

  @Override
  public Map<String, Long> countAggregation(AggregationQuery aggregationQuery) {
    return aggregationsCache.get(aggregationQuery);
  }

}
