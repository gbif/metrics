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
package org.gbif.metrics.es;

import org.gbif.metrics.MetricsCacheService;
import org.gbif.metrics.MetricsService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.expiry.Expiry;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
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

import lombok.Data;
import lombok.NoArgsConstructor;

/** MetricsSevice based on Elasticsearch. */
public class EsMetricsService implements MetricsService, MetricsCacheService {

  private static final Logger LOG = LoggerFactory.getLogger(EsMetricsService.class);

  private static final int AGG_SIZE = 30_000;
  private static final int SHARD_SIZE = 10_000;

  // Map of dimensions/parameter.name to Elasticsearch fields
  private static final Map<String, String> DIMENSION_TO_ES_FIELD;

  private final String defaultChecklistKey;

  static {
    Map<String, String> fieldsMap = new HashMap<>();
    fieldsMap.put("basisOfRecord", "basisOfRecord");
    fieldsMap.put("country", "countryCode");
    fieldsMap.put("isGeoreferenced", "hasCoordinate");
    fieldsMap.put("datasetKey", "datasetKey");
    fieldsMap.put("publishingCountry", "publishingCountry");
    fieldsMap.put("typeStatus", "typeStatus");
    fieldsMap.put("issue", "issues");
    fieldsMap.put("year", "year");
    fieldsMap.put("protocol","protocol");
    fieldsMap.put("checklistKey","checklistKey");
    DIMENSION_TO_ES_FIELD = Collections.unmodifiableMap(fieldsMap);
  }

  // Cache for count queries
  private final Cache<CountQuery, Long> countCache;

  // Cache for aggregation queries
  private final Cache<AggregationQuery, Map<String, Long>> aggregationsCache;

  private final String esIndex;

  private final RestHighLevelClient esClient;

  private static Optional<Parameter> getChecklistKeyParameter(Collection<Parameter> parameters) {
    return parameters.stream()
        .filter(p -> p.getName().equalsIgnoreCase("checklistKey"))
        .findFirst();
  }

  private String getDimensionToEsField(AggregationQuery aggregationQuery) {
    Optional<Parameter> checklistKeyParam = getChecklistKeyParameter(aggregationQuery.getParameters());
    if (aggregationQuery.getDimension().equalsIgnoreCase("kingdom")) {
      String checklistKey = checklistKeyParam.map(parameter -> parameter.getValue().toString()).orElse(defaultChecklistKey);
      return "classifications." + checklistKey + ".kingdom.classificationKeys.KINGDOM";
    }
    return DIMENSION_TO_ES_FIELD.get(aggregationQuery.getDimension());
  }


  /**
   * Consolidated helper for determining ES field for a parameter using a collection of parameters
   * (so it can be reused for CountQuery and AggregationQuery).
   */
  private String getDimensionToEsField(Parameter parameter, Collection<Parameter> ctxParameters) {
    Optional<Parameter> checklistKeyParamOpt = getChecklistKeyParameter(ctxParameters);
    if (parameter.getName().equalsIgnoreCase("taxonKey")) {
      String checklistKey = checklistKeyParamOpt.map(checklistKeyParam -> checklistKeyParam.getValue().toString()).orElse(defaultChecklistKey);
      return "classifications." + checklistKey + ".taxonKeys";
    }
    return DIMENSION_TO_ES_FIELD.get(parameter.getName());
  }

  @Data
  @NoArgsConstructor
  public static class CacheConfig {
    private long expireAfterWrite;
    private long entryCapacity;
    private boolean refreshAhead;
  }

  public EsMetricsService(String esIndex, CacheConfig cacheConfig, RestHighLevelClient esClient, String defaultChecklistKey ) {
    this.esIndex = esIndex;
    this.esClient = esClient;
    this.defaultChecklistKey = defaultChecklistKey;
    countCache =
        new Cache2kBuilder<CountQuery, Long>() {}.loader(this::loadCount)
            .expireAfterWrite(cacheConfig.expireAfterWrite, TimeUnit.MILLISECONDS)
            .refreshAhead(cacheConfig.refreshAhead)
            .entryCapacity(cacheConfig.entryCapacity)
            .build();

    aggregationsCache =
        new Cache2kBuilder<AggregationQuery, Map<String, Long>>() {}.loader(this::loadAggregation)
            .expireAfterWrite(cacheConfig.expireAfterWrite, TimeUnit.MILLISECONDS)
            .refreshAhead(cacheConfig.refreshAhead)
            .entryCapacity(cacheConfig.entryCapacity)
            .build();
  }

  /** Loader function for the count queries cache. */
  private Long loadCount(CountQuery countQuery) {
    try {
      return esClient.count(buildCountRequest(countQuery), RequestOptions.DEFAULT).getCount();
    } catch (IOException ex) {
      LOG.error("Error executing CountQuery {}", countQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  /** Loader function for the aggregation queries cache. */
  private Map<String, Long> loadAggregation(AggregationQuery aggregationQuery) {
    try {
      SearchResponse response =
          esClient.search(buildCountsAggregateRequest(aggregationQuery), RequestOptions.DEFAULT);
      List<? extends Terms.Bucket> buckets =
          ((Terms) response.getAggregations().get(aggregationQuery.getDimension())).getBuckets();
      Map<String, Long> aggregation = new LinkedHashMap<>(buckets.size());
      // Results added in reverse order because the ES API returns them like that
      for (int i = buckets.size() - 1; i >= 0; i--) {
        Terms.Bucket bucket = buckets.get(i);
        aggregation.put(
            aggregationQuery.getKeyLabelTransform().apply(bucket.getKeyAsString()),
            bucket.getDocCount());
      }
      return aggregation;
    } catch (IOException ex) {
      LOG.error("Error executing AggregationQuery {}", aggregationQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  /** Builds an Elasticsearch {@link CountRequest} from a {@link CountQuery}. */
  private CountRequest buildCountRequest(CountQuery countQuery) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    countQuery.getParameters().forEach(p -> bool.filter().add(buildQuery(p, countQuery)));
    CountRequest countRequest = new CountRequest();
    countRequest.query(bool);
    countRequest.indices(esIndex);
    return countRequest;
  }

  /**
   * Builds a {@link SearchRequest} with the aggregation parameters from a {@link AggregationQuery}.
   */
  private SearchRequest buildCountsAggregateRequest(AggregationQuery aggregationQuery) {
    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms(aggregationQuery.getDimension())
            .order(BucketOrder.count(true)) // Order by count
            .field(getDimensionToEsField(aggregationQuery))
            .size(AGG_SIZE)
            .shardSize(SHARD_SIZE);
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    aggregationQuery
        .getParameters()
        .forEach(parameter -> boolQueryBuilder.filter(buildQuery(parameter, aggregationQuery)));
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(
        boolQueryBuilder.filter().isEmpty() ? QueryBuilders.matchAllQuery() : boolQueryBuilder);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(aggregation);
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(esIndex);
    return searchRequest;
  }

  /** Consolidated query builder that uses the provided context parameters to resolve ES fields. */
  private QueryBuilder buildQuery(Parameter parameter, Collection<Parameter> ctxParameters) {
    if (parameter.getValue() instanceof YearRange) {
      return QueryBuilders.rangeQuery(getDimensionToEsField(parameter, ctxParameters))
        .gte(((YearRange) parameter.getValue()).getStartYear())
        .lte(((YearRange) parameter.getValue()).getEndYear());
    }
    if ((parameter.getValue().getClass().equals(String.class)
        && parameter.getValue().toString().contains(","))) {
      String[] values = parameter.getValue().toString().split(",");

      return QueryBuilders.rangeQuery(getDimensionToEsField(parameter, ctxParameters))
        .gte(values[0])
        .lte(values[1]);
    }
    return QueryBuilders.termQuery(
      getDimensionToEsField(parameter, ctxParameters), parameter.getValue());
  }

  // Small delegating wrappers to keep existing usage sites unchanged.
  private QueryBuilder buildQuery(Parameter parameter, AggregationQuery aggregationQuery) {
    return buildQuery(parameter, aggregationQuery.getParameters());
  }

  private QueryBuilder buildQuery(Parameter parameter, CountQuery countQuery) {
    return buildQuery(parameter, countQuery.getParameters());
  }

  @Override
  public void flush() {
    countCache.removeAll();
    aggregationsCache.removeAll();
  }

  @Override
  public void refresh(CountQuery countQuery) {
    LOG.info("Expiring and refreshing count query {}", countQuery);
    countCache.invoke(countQuery, e -> e.setExpiryTime(Expiry.REFRESH));
  }

  @Override
  public void refresh(AggregationQuery aggregationQuery) {
    LOG.info("Expiring and refreshing aggregation query {}", aggregationQuery);
    aggregationsCache.invoke(aggregationQuery, e -> e.setExpiryTime(Expiry.REFRESH));
  }

  @Override
  public Long count(CountQuery countQuery) {
    return countCache.get(countQuery);
  }

  @Override
  public Map<String, Long> countAggregation(AggregationQuery aggregationQuery) {
    return aggregationsCache.get(aggregationQuery);
  }
}
