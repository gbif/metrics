package org.gbif.metrics.cube.occurrence.backfill;


import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.hbase.HBaseFieldUtil;

import java.io.IOException;

import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * The callback used from the backfill process to spawn the job to write the new data in the cube.
 * This is a simple 1 pass scan of the occurrence data which writes the cube.
 */
class BackfillCallback implements HBaseBackfillCallback {

  @Override
  public void backfillInto(Configuration conf, byte[] table, byte[] cf, long snapshotFinishMs) throws IOException {
    conf = HBaseConfiguration.create(conf);

    try {
      Job job = new Job(conf, "occurrence cube writer");
      job.setJarByClass(TableReaderMapper.class); // required to set up MR classpath
      // These are set high, as any failure means inaccurate cube counts
      job.getConfiguration().set("mapred.task.timeout", "14400000"); // 240 mins (writes can block a long time)
      job.getConfiguration().set("hbase.regionserver.lease.period", "600000"); // 10 mins
      job.getConfiguration().set("mapred.reduce.max.attempts", "1"); // any failures mean invalid counts
      job.setCombinerClass(AddressCombiner.class);
      job.setNumReduceTasks(50);

      TableMapReduceUtil.initTableMapperJob(conf.get(HBaseSourcedBackfill.KEY_SOURCE_TABLE), getScanner(conf),
        TableReaderMapper.class, ImmutableBytesWritable.class, IntWritable.class, job);
      TableMapReduceUtil.initTableReducerJob(conf.get(HBaseSourcedBackfill.KEY_BACKFILL_TABLE),
        CubeWriterReducer.class, job);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Unknown error with job.  Check the logs.");
      }

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void addFieldToScan(Scan scan, FieldName fn) {
    scan.addColumn(Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fn).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fn).getColumnName()));
  }

  private Scan getScanner(Configuration conf) {
    Scan scan = new Scan();
    scan.setCaching(conf.getInt(HBaseSourcedBackfill.KEY_SCANNER_CACHE, HBaseSourcedBackfill.DEFAULT_SCANNER_CACHE));
    scan.setCacheBlocks(false); // not needed for efficient scanning
    // Optimize the scan by bringing back only what the CubeWriterMapper wants
    addFieldToScan(scan, FieldName.I_KINGDOM_ID);
    addFieldToScan(scan, FieldName.I_PHYLUM_ID);
    addFieldToScan(scan, FieldName.I_CLASS_ID);
    addFieldToScan(scan, FieldName.I_ORDER_ID);
    addFieldToScan(scan, FieldName.I_FAMILY_ID);
    addFieldToScan(scan, FieldName.I_GENUS_ID);
    addFieldToScan(scan, FieldName.I_SPECIES_ID);
    addFieldToScan(scan, FieldName.I_NUB_ID);
    addFieldToScan(scan, FieldName.DATASET_KEY);
    addFieldToScan(scan, FieldName.I_ISO_COUNTRY_CODE);
    addFieldToScan(scan, FieldName.HOST_COUNTRY);
    addFieldToScan(scan, FieldName.I_LATITUDE);
    addFieldToScan(scan, FieldName.I_LONGITUDE);
    addFieldToScan(scan, FieldName.I_YEAR);
    addFieldToScan(scan, FieldName.I_MONTH);
    addFieldToScan(scan, FieldName.I_BASIS_OF_RECORD);
    addFieldToScan(scan, FieldName.HOST_COUNTRY);
    addFieldToScan(scan, FieldName.OWNING_ORG_KEY);
    addFieldToScan(scan, FieldName.PROTOCOL);
    addFieldToScan(scan, FieldName.I_GEOSPATIAL_ISSUE);
    return scan;
  }
}
