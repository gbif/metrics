package org.gbif.metrics.cube.occurrence;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;

import java.util.Map;
import java.util.UUID;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.WriteBuilder;
import com.urbanairship.datacube.ops.LongOp;

import static org.gbif.metrics.cube.occurrence.OccurrenceCube.BASIS_OF_RECORD;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.COUNTRY;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.DATASET_KEY;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.HOST_COUNTRY;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.IS_GEOREFERENCED;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.NUB_KEY;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.PROTOCOL;
import static org.gbif.metrics.cube.occurrence.OccurrenceCube.YEAR;

/**
 * A utility class to encapsulate the logic needed to convert between the denormalized
 * format of occurrences (which have kingdomKey, phylumKey etc) and the normalized format
 * used by the occurrence cube (which uses only a nubKey).
 * This class uses the occurrence cube rollup definitions to determine which cube addresses
 * are to be updated for a given record. This is only necessary due to the normalization
 * that is required to for the taxonomic ranks that are all merged into a single nub key.
 * Brief explanation:
 * - Imagine a record from dataset:1 with kingdom:1 and phylum:2
 * - Imagine the cube counts "by dataset", "records by nubKey" and "records by dataset by nubKey"
 * We need to update the following cube addresses:
 * - "dataset:1"
 * - "nubKey:1"
 * - "nubKey:2"
 * - "dataset:1,nubKey:1"
 * - "dataset:1,nubKey:2"
 * You cannot update the cube 2 times for each taxon, or else the dataset count will be 2 in this
 * example.
 * This is the problem this class addresses, by providing the addresses for a single occurrence record.
 */
public class OccurrenceAddressUtil {

  /**
   * For the given occurrence, determines the mutations (addresses and operations) that need
   * to be applied.
   * 
   * @param occurrence The denormalized representation
   * @param op That is going to be applied to the cube
   * @return The batch of updates to apply
   */
  public static Batch<LongOp> cubeMutation(Occurrence occurrence, LongOp op) {
    // Needs mutability, since we rely on overwriting as a lazy distinction mechanism
    // such as for when speciesKey = nubKey
    Map<Address, LongOp> m = Maps.newHashMap();
    m.putAll(mutationsForTaxon(occurrence, occurrence.getKingdomKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getPhylumKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getClassKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getOrderKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getFamilyKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getGenusKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getSpeciesKey(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getNubKey(), op));
    return new Batch<LongOp>(m);
  }

  /**
   * For the given occurrence writable, determines the mutations (addresses and operations) that need
   * to be applied.
   * 
   * @param occurrence The writable representation
   * @param op That is going to be applied to the cube
   * @return The batch of updates to apply
   */
  public static Batch<LongOp> cubeMutation(OccurrenceWritable occurrence, LongOp op) {
    // Needs mutability, since we rely on overwriting as a lazy distinction mechanism
    // such as for when speciesKey = nubKey
    Map<Address, LongOp> m = Maps.newHashMap();
    m.putAll(mutationsForTaxon(occurrence, occurrence.getKingdomID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getPhylumID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getClassID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getOrderID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getFamilyID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getGenusID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getSpeciesID(), op));
    m.putAll(mutationsForTaxon(occurrence, occurrence.getTaxonID(), op));
    return new Batch<LongOp>(m);
  }

  /**
   * For the given taxon, returns all the updates to be applied to the cube
   */
  private static Map<Address, LongOp> mutationsForTaxon(Occurrence occurrence, Integer nubKey, LongOp op) {
    WriteBuilder wb = new WriteBuilder(OccurrenceCube.INSTANCE);
    addEnumDimension(wb, occurrence.getCountry(), COUNTRY);
    addEnumDimension(wb, occurrence.getProtocol(), PROTOCOL);
    addGeoreferencingDimension(wb, occurrence, IS_GEOREFERENCED);
    addEnumDimension(wb, occurrence.getBasisOfRecord(), BASIS_OF_RECORD);
    addUUIDDimension(wb, occurrence.getDatasetKey(), DATASET_KEY);
    addEnumDimension(wb, occurrence.getHostCountry(), HOST_COUNTRY);
    addIntDimension(wb, nubKey, NUB_KEY);
    addIntDimension(wb, occurrence.getOccurrenceYear(), YEAR);
    return OccurrenceCube.INSTANCE.getWrites(wb, op).getMap();
  }

  /**
   * For the given taxon, returns all the updates to be applied to the cube
   */
  private static Map<Address, LongOp> mutationsForTaxon(OccurrenceWritable occurrence, Integer nubKey, LongOp op) {
    WriteBuilder wb = new WriteBuilder(OccurrenceCube.INSTANCE);
    addCountryEnumDimension(wb, occurrence.getCountryIsoCode(), COUNTRY);
    addEnumDimension(wb, occurrence.getProtocol(), PROTOCOL);
    addGeoreferencingDimension(wb, occurrence, IS_GEOREFERENCED);
    addEnumDimension(wb, occurrence.getBasisOfRecord(), BASIS_OF_RECORD);
    addUUIDDimension(wb, occurrence.getDatasetKey(), DATASET_KEY);
    addCountryEnumDimension(wb, occurrence.getHostCountryIsoCode(), HOST_COUNTRY);
    addIntDimension(wb, nubKey, NUB_KEY);
    addIntDimension(wb, occurrence.getYear(), YEAR);
    return OccurrenceCube.INSTANCE.getWrites(wb, op).getMap();
  }

  private static WriteBuilder addGeoreferencingDimension(WriteBuilder wb, Occurrence occurrence,
    Dimension<Boolean> isGeoreferenced) {
    return (occurrence.getLatitude() != null && occurrence.getLongitude() != null &&
      (occurrence.getGeospatialIssue() == null || 0 == occurrence.getGeospatialIssue())) ? wb.at(IS_GEOREFERENCED, true) : wb
     .at(IS_GEOREFERENCED, false);  }

  private static WriteBuilder addGeoreferencingDimension(WriteBuilder wb, OccurrenceWritable occurrence,
    Dimension<Boolean> isGeoreferenced) {
    return (occurrence.getLatitude() != null && occurrence.getLongitude() != null &&
      0 == occurrence.getIssues()) ? wb.at(IS_GEOREFERENCED, true) : wb
     .at(IS_GEOREFERENCED, false);  }
  
  private static WriteBuilder addUUIDDimension(WriteBuilder wb, UUID i, Dimension<UUID> dim) {
    return (i != null) ? wb.at(dim, i) : wb;
  }

  private static WriteBuilder addUUIDDimension(WriteBuilder wb, String s, Dimension<UUID> dim) {
    return (s != null) ? wb.at(dim, UUID.fromString(s)) : wb;
  }

  private static WriteBuilder addIntDimension(WriteBuilder wb, Integer i, Dimension<Integer> dim) {
    return (i != null) ? wb.at(dim, i) : wb;
  }

  private static <T extends Enum<?>> WriteBuilder addEnumDimension(WriteBuilder wb, T e, Dimension<T> dim) {
    return (e != null) ? wb.at(dim, e) : wb;
  }

  private static <T extends Enum<?>> WriteBuilder addCountryEnumDimension(WriteBuilder wb, String s,
    Dimension<Country> dim) {
    if (!Strings.isNullOrEmpty(s)) {
      Country c = Country.fromIsoCode(s);
      return (c != null) ? wb.at(dim, c) : wb; // skip unparsable countries
    }
    return wb;
  }
}
