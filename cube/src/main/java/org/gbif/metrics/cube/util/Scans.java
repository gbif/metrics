package org.gbif.metrics.cube.util;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.api.InternalTerm;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class Scans {
  private Scans() {
  }

  public static void addSpatialIssueColumns(Scan scan) {
    for (OccurrenceIssue issue : OccurrenceIssue.GEOSPATIAL_RULES) {
      scan.addColumn(Columns.CF, Bytes.toBytes(Columns.column(issue)));
    }
  }

  public static void addTerm(Scan scan, Term term) {
    scan.addColumn(Columns.CF, Bytes.toBytes(Columns.column(term)));
  }

  /**
   * Adds all taxonomic classificaiton key terms plus the main taxonKey to a scan.
   */
  public static void addTaxonomyColumns(Scan scan) {
    addTerm(scan, GbifTerm.taxonKey);
    for (Term taxKeyTerm : OccurrenceBuilder.rank2KeyTerm.values()) {
      addTerm(scan, taxKeyTerm);
    }
  }

  public static void addCoordinateColumns(Scan scan) {
    addTerm(scan, DwcTerm.decimalLatitude);
    addTerm(scan, DwcTerm.decimalLongitude);
  }

  public static void addOtherColumns(Scan scan) {
    addTerm(scan, GbifTerm.datasetKey);
    addTerm(scan, InternalTerm.publishingOrgKey);
    addTerm(scan, GbifTerm.publishingCountry);
    addTerm(scan, DwcTerm.countryCode);
    addTerm(scan, DwcTerm.year);
    addTerm(scan, DwcTerm.month);
    addTerm(scan, DwcTerm.basisOfRecord);
    addTerm(scan, GbifTerm.protocol);
    addTerm(scan, DwcTerm.typeStatus);
  }
}
