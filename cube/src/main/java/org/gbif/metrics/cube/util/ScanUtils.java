package org.gbif.metrics.cube.util;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.hbase.HBaseColumn;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class ScanUtils {

  private ScanUtils() {
  }

  public static void addSpatialIssueColumns(Scan scan) {
    for (OccurrenceIssue issue : OccurrenceIssue.GEOSPATIAL_RULES) {
      HBaseColumn column = HBaseFieldUtil.getHBaseColumn(issue);
      scan.addColumn(Bytes.toBytes(column.getFamilyName()), Bytes.toBytes(column.getColumnName()));
    }
  }

  public static void addFieldToScan(Scan scan, FieldName... fields) {
    Preconditions.checkNotNull(fields);
    for (FieldName fn : fields) {
      HBaseColumn column = HBaseFieldUtil.getHBaseColumn(fn);
      scan.addColumn(Bytes.toBytes(column.getFamilyName()), Bytes.toBytes(column.getColumnName()));
    }
  }

  public static void addTaxonomyColumns(Scan scan) {
    addFieldToScan(scan, FieldName.I_KINGDOM_KEY, FieldName.I_PHYLUM_KEY, FieldName.I_CLASS_KEY, FieldName.I_ORDER_KEY,
      FieldName.I_FAMILY_KEY, FieldName.I_GENUS_KEY, FieldName.I_SUBGENUS_KEY, FieldName.I_SPECIES_KEY,
      FieldName.I_TAXON_KEY);
  }

  public static void addCoordinateColumns(Scan scan) {
    addFieldToScan(scan, FieldName.I_DECIMAL_LATITUDE, FieldName.I_DECIMAL_LONGITUDE);
  }

  public static void addOtherColumns(Scan scan) {
    addFieldToScan(scan, FieldName.PUB_ORG_KEY);
    addFieldToScan(scan, FieldName.DATASET_KEY);
    addFieldToScan(scan, FieldName.I_COUNTRY);
    addFieldToScan(scan, FieldName.PUB_COUNTRY_CODE);
    addFieldToScan(scan, FieldName.I_YEAR);
    addFieldToScan(scan, FieldName.I_MONTH);
    addFieldToScan(scan, FieldName.I_BASIS_OF_RECORD);
    addFieldToScan(scan, FieldName.PROTOCOL);
    addFieldToScan(scan, FieldName.I_TYPE_STATUS);
  }
}
