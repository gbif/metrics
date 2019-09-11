package org.gbif.metrics.es;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class EsMetricsService {

  private static final Map<String,String> DIMENSION_TO_ES_FIELD;

  static {
    Map<String,String> fieldsMap = new HashMap<>();
    fieldsMap.put("basisOfRecord", "basisOfRecord");
    fieldsMap.put("country", "country");
    fieldsMap.put("isGeoreferenced", "hasCoordinate");
    fieldsMap.put("taxonKey", "taxonKey");
    fieldsMap.put("datasetKey", "datasetKey");
    fieldsMap.put("publishingCountry", "datasetPublishingCountry");
    fieldsMap.put("typeStatus", "typeStatus");
    fieldsMap.put("issue", "issues");
    fieldsMap.put("year", "year");
    DIMENSION_TO_ES_FIELD = Collections.unmodifiableMap(fieldsMap);
  }

  public static class Parameter {


    private final String name;
    private final String value;

    public Parameter(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getEsField() {
      return DIMENSION_TO_ES_FIELD.get(name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Parameter parameter = (Parameter) o;
      return name.equals(parameter.name) && value.equals(parameter.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }
  }


  public static class AggregationQuery {

    public static final  AggregationQuery BASIS_OF_RECORD = new AggregationQuery("basisOfRecord", Collections.EMPTY_LIST);

    private final String dimension;

    private final Collection<Parameter> parameters;

    public AggregationQuery(String dimension, Collection<Parameter> parameters) {
      this.dimension = dimension;
      this.parameters = parameters;
    }

    public String getDimension() {
      return dimension;
    }

    public String getEsField() {
      return DIMENSION_TO_ES_FIELD.get(dimension);
    }

    public Collection<Parameter> getParameters() {
      return parameters;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AggregationQuery that = (AggregationQuery) o;
      return dimension.equals(that.dimension) && parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dimension, parameters);
    }
  }

  public static class CountQuery {

    private final Map<Parameter,Long> parameters;

    public CountQuery(Map<Parameter,Long> parameters) {
      this.parameters = parameters;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CountQuery that = (CountQuery) o;
      return that.parameters.entrySet().equals(this.parameters.entrySet());
    }

    @Override
    public int hashCode() {
      return Objects.hash(parameters.entrySet());
    }

    public Map<Parameter, Long> getParameters() {
      return parameters;
    }
  }


  //Caches information by DOI
  private final Cache<CountQuery, Long> countCache;

  private final Cache<AggregationQuery, Map<String,Long>> aggregationsCache;

  private final EsConfig esConfig;

  private final RestHighLevelClient restClient;

  public EsMetricsService(EsConfig esConfig) {
    this.esConfig = esConfig;
    restClient = buildClient(esConfig);
    countCache = new Cache2kBuilder<CountQuery,Long>(){}
      .loader(new CacheLoader<CountQuery, Long>() {
        @Override
        public Long load(final CountQuery key) throws Exception {
          return loadCount(key);
        }
      })
      .expireAfterWrite(1, TimeUnit.HOURS)
      .build();

    aggregationsCache = new Cache2kBuilder<AggregationQuery, Map<String,Long>>(){}
      .loader(new CacheLoader<AggregationQuery, Map<String,Long>>() {
        @Override
        public Map<String,Long> load(final AggregationQuery key) throws Exception {
          return loadAggregation(key);
        }
      })
      .expireAfterWrite(1, TimeUnit.HOURS)
      .build();
  }

  private Long loadCount(CountQuery countQuery) {
    try {
      return restClient.count(buildCountRequest(countQuery), RequestOptions.DEFAULT).getCount();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Map<String,Long> loadAggregation(AggregationQuery aggregationQuery) {
    try {
      SearchResponse response = restClient.search(buildCountsAggregateRequest(aggregationQuery), RequestOptions.DEFAULT);
      return ((Terms)response.getAggregations().get(aggregationQuery.getEsField())).getBuckets().stream().collect(Collectors.toMap(Terms.Bucket::getKeyAsString, Terms.Bucket::getDocCount));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }


  private CountRequest buildCountRequest(CountQuery countQuery) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    countQuery.getParameters().forEach((p, v) -> bool.filter().add(buildQuery(p)));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(bool);
    CountRequest countRequest = new CountRequest();
    countRequest.source(searchSourceBuilder);
    countRequest.indices(esConfig.getIndexName());
    return countRequest;
  }

  private SearchRequest buildCountsAggregateRequest(AggregationQuery aggregationQuery) {
    TermsAggregationBuilder aggregation = AggregationBuilders.terms(aggregationQuery.getEsField()).shardSize(10_000);
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    aggregationQuery.getParameters().forEach(parameter -> boolQueryBuilder.filter(buildQuery(parameter)));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder.filter().isEmpty()? QueryBuilders.matchAllQuery() : boolQueryBuilder);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(aggregation);
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(esConfig.getIndexName());
    return searchRequest;
  }


  private QueryBuilder buildQuery(Parameter parameter) {
    if (parameter.getValue().contains(",")) {
      String[] values = parameter.getValue().split(",");
      return QueryBuilders.rangeQuery(parameter.getEsField()).gte(values[0]).lte(values[1]);
    }
    return QueryBuilders.termQuery(parameter.getEsField(), parameter.getValue());
  }

  private static RestHighLevelClient buildClient(EsConfig esConfig) {
    Objects.requireNonNull(esConfig);
    Objects.requireNonNull(esConfig.getHosts());
    Preconditions.checkArgument(!esConfig.getHosts().isEmpty());

    HttpHost[] hosts = new HttpHost[esConfig.getHosts().size()];
    for (int i = 0; i < esConfig.getHosts().size(); i++) {
      URL urlHost = esConfig.getHosts().get(i);
      hosts[i] = new HttpHost(urlHost.getHost(), urlHost.getPort(), urlHost.getProtocol());
    }

    return new RestHighLevelClient(RestClient.builder(hosts).setMaxRetryTimeoutMillis(180_000));
  }

  public Long count(CountQuery countQuery) {
    return countCache.get(countQuery);
  }


  public Map<String, Long> countAggregation(AggregationQuery aggregationQuery) {
    return aggregationsCache.get(aggregationQuery);
  }

}
