package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.tile.MercatorProjectionUtil;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.OccurrenceResultReader;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/**
 * Reads the HBase table, collecting the terms we are interested in as we know there are duplicates
 * at the same location. This is only to reduce the amount of grouping needed in later MR jobs.
 */
public class LocationCollectorMapper extends TableMapper<OccurrenceWritable, IntWritable> {

  private final static IntWritable ONE = new IntWritable(1);
  private final BasisOfRecordConverter borc = new BasisOfRecordConverter();

  /**
   * Reads the table, emits the Result keyed on the Lat Lng if it is a plottable record
   */
  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {

    Double latitude = OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE);
    Double longitude = OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE);

    // Google only goes +/- 85 degrees and we only want maps with no known issues
    if (!OccurrenceWritable.hasSpatialIssue(row) && MercatorProjectionUtil.isPlottable(latitude, longitude)) {

      // Note: Make sure everything read here is in the getScanner() in BackFillCallback!
      Integer kingdomID = OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID);
      Integer phylumID = OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID);
      Integer classID = OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID);
      Integer orderID = OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID);
      Integer familyID = OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID);
      Integer genusID = OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID);
      Integer speciesID = OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID);
      // taxon != species (it may be a higher taxon, or might be a subspecies)
      Integer taxonID = OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID);
      String publishingOrganisationKey = OccurrenceResultReader.getString(row, FieldName.OWNING_ORG_KEY);
      String datasetKey = OccurrenceResultReader.getString(row, FieldName.DATASET_KEY);
      String countryIsoCode = OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE);
      String hostCountryIsoCode = OccurrenceResultReader.getString(row, FieldName.HOST_COUNTRY);
      Integer year = OccurrenceResultReader.getInteger(row, FieldName.I_YEAR);

      Integer borAsInt = OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD);
      BasisOfRecord bor = borc.toEnum(borAsInt);
      EndpointType protocol = EndpointType.valueOf(OccurrenceResultReader.getString(row, FieldName.PROTOCOL));

      context.getCounter(bor).increment(1);

      context.write(new OccurrenceWritable(kingdomID, phylumID, classID, orderID, familyID, genusID, speciesID,
        taxonID, false, publishingOrganisationKey, datasetKey, countryIsoCode, hostCountryIsoCode, latitude,
        longitude, year, null, // month
        bor, protocol, 1), ONE);
    }
  }
}
