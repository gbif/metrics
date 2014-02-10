package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;

import com.google.common.base.Objects;

/**
 * A simple util for comparing occurrences for the purposes of metrics evaluation.
 */
public class OccurrenceComparisonUtil {

  /**
   * Should never be instantiated.
   */
  private OccurrenceComparisonUtil() {
  }

  /**
   * True if the subset of fields of occurrence, that are interesting to metrics and cube-updating, are the same.
   */
  public static boolean equivalent(Occurrence occ1, Occurrence occ2) {
    // TODO: Do we need to compare the holy triple too?
    /*
     * && Objects.equal(occ1.getInstitutionCode(), occ2.getInstitutionCode())
     * && Objects.equal(occ1.getCollectionCode(), occ2.getCollectionCode())
     * && Objects.equal(occ1.getCatalogNumber(), occ2.getCatalogNumber())
     */

    return Objects.equal(occ1.getKey(), occ2.getKey())
      && Objects.equal(occ1.getDatasetKey(), occ2.getDatasetKey())
      && Objects.equal(occ1.getPublishingOrgKey(), occ2.getPublishingOrgKey())
      && Objects.equal(occ1.getBasisOfRecord(), occ2.getBasisOfRecord())
      && Objects.equal(occ1.getKingdomKey(), occ2.getKingdomKey())
      && Objects.equal(occ1.getPhylumKey(), occ2.getPhylumKey())
      && Objects.equal(occ1.getClassKey(), occ2.getClassKey())
      && Objects.equal(occ1.getOrderKey(), occ2.getOrderKey())
      && Objects.equal(occ1.getFamilyKey(), occ2.getFamilyKey())
      && Objects.equal(occ1.getGenusKey(), occ2.getGenusKey())
      && Objects.equal(occ1.getSpeciesKey(), occ2.getSpeciesKey())
      && Objects.equal(occ1.getScientificName(), occ2.getScientificName())
      && Objects.equal(occ1.hasSpatialIssue(), occ2.hasSpatialIssue())
      && Objects.equal(occ1.getDecimalLatitude(), occ2.getDecimalLatitude())
      && Objects.equal(occ1.getDecimalLongitude(), occ2.getDecimalLongitude())
      && Objects.equal(occ1.getCountry(), occ2.getCountry())
      && Objects.equal(occ1.getPublishingCountry(), occ2.getPublishingCountry())
      && Objects.equal(occ1.getProtocol(), occ2.getProtocol());
  }
}
