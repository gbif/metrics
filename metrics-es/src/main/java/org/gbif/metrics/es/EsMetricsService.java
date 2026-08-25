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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.LongTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.NamedValue;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.expiry.Expiry;
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
    fieldsMap.put("protocol", "protocol");
    fieldsMap.put("checklistKey", "checklistKey");
    DIMENSION_TO_ES_FIELD = Collections.unmodifiableMap(fieldsMap);
  }

  // Cache for count queries
  private final Cache<CountQuery, Long> countCache;

  // Cache for aggregation queries
  private final Cache<AggregationQuery, Map<String, Long>> aggregationsCache;

  private final String esIndex;

  private final ElasticsearchClient esClient;

  private static Optional<Parameter> getChecklistKeyParameter(Collection<Parameter> parameters) {
    return parameters.stream()
        .filter(p -> p.getName().equalsIgnoreCase("checklistKey"))
        .findFirst();
  }

  private String getDimensionToEsField(AggregationQuery aggregationQuery) {
    Optional<Parameter> checklistKeyParam =
        getChecklistKeyParameter(aggregationQuery.getParameters());
    if (aggregationQuery.getDimension().equalsIgnoreCase("kingdom")) {
      String checklistKey =
          checklistKeyParam
              .map(parameter -> parameter.getValue().toString())
              .orElse(defaultChecklistKey);
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
      String checklistKey =
          checklistKeyParamOpt
              .map(checklistKeyParam -> checklistKeyParam.getValue().toString())
              .orElse(defaultChecklistKey);
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

  public EsMetricsService(
      String esIndex,
      CacheConfig cacheConfig,
      ElasticsearchClient esClient,
      String defaultChecklistKey) {
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
      CountResponse response =
          esClient.count(c -> c.index(esIndex).query(buildBoolQuery(countQuery.getParameters())));
      return response.count();
    } catch (IOException ex) {
      LOG.error("Error executing CountQuery {}", countQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  /** Loader function for the aggregation queries cache. */
  private Map<String, Long> loadAggregation(AggregationQuery aggregationQuery) {
    try {
      String dimension = aggregationQuery.getDimension();
      SearchResponse<Void> response =
          esClient.search(
              s ->
                  s.index(esIndex)
                      .size(0)
                      .query(buildAggregationQuery(aggregationQuery))
                      .aggregations(
                          dimension,
                          a ->
                              a.terms(
                                  t ->
                                      t.field(getDimensionToEsField(aggregationQuery))
                                          .size(AGG_SIZE)
                                          .shardSize(SHARD_SIZE)
                                          .order(
                                              NamedValue.of("_count", SortOrder.Asc)))),
              Void.class);

      Aggregate aggregate = response.aggregations().get(dimension);
      List<Map.Entry<String, Long>> buckets = extractTermsBuckets(aggregate);
      Map<String, Long> aggregation = new LinkedHashMap<>(buckets.size());
      // Results added in reverse order because the ES API returns them like that
      for (int i = buckets.size() - 1; i >= 0; i--) {
        Map.Entry<String, Long> bucket = buckets.get(i);
        aggregation.put(
            aggregationQuery.getKeyLabelTransform().apply(bucket.getKey()), bucket.getValue());
      }
      return aggregation;
    } catch (IOException ex) {
      LOG.error("Error executing AggregationQuery {}", aggregationQuery, ex);
      throw new RuntimeException(ex);
    }
  }

  private static List<Map.Entry<String, Long>> extractTermsBuckets(Aggregate aggregate) {
    List<Map.Entry<String, Long>> buckets = new ArrayList<>();
    if (aggregate.isSterms()) {
      for (StringTermsBucket bucket : aggregate.sterms().buckets().array()) {
        // Same as HLRC Terms.Bucket#getKeyAsString()
        buckets.add(Map.entry(termsBucketKeyAsString(bucket.key()), bucket.docCount()));
      }
    } else if (aggregate.isLterms()) {
      for (LongTermsBucket bucket : aggregate.lterms().buckets().array()) {
        String key =
            bucket.keyAsString() != null ? bucket.keyAsString() : Long.toString(bucket.key());
        buckets.add(Map.entry(key, bucket.docCount()));
      }
    } else {
      // e.g. dterms — not used by current facets, fail loudly if mapping type changes
      throw new IllegalStateException("Unexpected terms aggregation type: " + aggregate._kind());
    }
    return buckets;
  }

  /** Mirrors HLRC {@code getKeyAsString()} for string terms keys. */
  private static String termsBucketKeyAsString(FieldValue key) {
    if (key.isString()) {
      return key.stringValue();
    }
    if (key.isLong()) {
      return Long.toString(key.longValue());
    }
    if (key.isDouble()) {
      return Double.toString(key.doubleValue());
    }
    if (key.isBoolean()) {
      return Boolean.toString(key.booleanValue());
    }
    if (key.isNull()) {
      return "null";
    }
    return key.toString();
  }

  private Query buildAggregationQuery(AggregationQuery aggregationQuery) {
    List<Query> filters = new ArrayList<>();
    aggregationQuery
        .getParameters()
        .forEach(parameter -> filters.add(buildQuery(parameter, aggregationQuery.getParameters())));
    if (filters.isEmpty()) {
      return Query.of(q -> q.matchAll(m -> m));
    }
    return Query.of(q -> q.bool(b -> b.filter(filters)));
  }

  private Query buildBoolQuery(Collection<Parameter> parameters) {
    List<Query> filters = new ArrayList<>();
    parameters.forEach(p -> filters.add(buildQuery(p, parameters)));
    return Query.of(q -> q.bool(b -> b.filter(filters)));
  }

  /** Consolidated query builder that uses the provided context parameters to resolve ES fields. */
  private Query buildQuery(Parameter parameter, Collection<Parameter> ctxParameters) {
    String field = getDimensionToEsField(parameter, ctxParameters);
    if (parameter.getValue() instanceof YearRange yearRange) {
      return Query.of(
          q ->
              q.range(
                  r ->
                      r.number(
                          n ->
                              n.field(field)
                                  .gte((double) yearRange.getStartYear())
                                  .lte((double) yearRange.getEndYear()))));
    }
    if (parameter.getValue().getClass().equals(String.class)
        && parameter.getValue().toString().contains(",")) {
      String[] values = parameter.getValue().toString().split(",");
      return Query.of(
          q ->
              q.range(
                  r ->
                      r.untyped(
                          u ->
                              u.field(field)
                                  .gte(JsonData.of(values[0]))
                                  .lte(JsonData.of(values[1])))));
    }
    return Query.of(q -> q.term(t -> t.field(field).value(toFieldValue(parameter.getValue()))));
  }

  /** Maps {@link Parameter} values to ES term values (bool / number / enum name / string). */
  private static FieldValue toFieldValue(Object value) {
    if (value instanceof Boolean b) {
      return FieldValue.of(b);
    }
    if (value instanceof Number n) {
      return FieldValue.of(n.longValue());
    }
    if (value instanceof Enum<?> e) {
      return FieldValue.of(e.name());
    }
    return FieldValue.of(value.toString());
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
