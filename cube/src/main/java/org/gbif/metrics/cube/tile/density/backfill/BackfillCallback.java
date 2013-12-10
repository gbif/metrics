package org.gbif.metrics.cube.tile.density.backfill;


import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;
import org.gbif.metrics.cube.tile.io.TileKeyWritable;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.hbase.HBaseFieldUtil;

import java.io.IOException;
import java.util.UUID;

import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * The callback used from the backfill process to spawn the job to write the new data in the cube.
 * This runs 2 MR jobs:
 * i) Group the occurrence records at the same location with the same identification in the same dataset etc.
 * ii) Build the cube for all the dimensions.
 * MR job i) is purely to reduce load, as MR job ii) emits several tiles at each zoom level (= a lot to shuffle and
 * sort)
 */
class BackfillCallback implements HBaseBackfillCallback {

  private void addFieldToScan(Scan scan, FieldName fn) {
    scan.addColumn(Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fn).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fn).getColumnName()));
  }

  @Override
  public void backfillInto(Configuration conf, byte[] table, byte[] cf, long snapshotFinishMs) throws IOException {
    conf = HBaseConfiguration.create(conf);
    conf.set("mapred.reduce.slowstart.completed.maps", "1.0"); // delay reducers to spare the machine load

    // a temporary directory for using to store intermediate data between the 2 jobs
    String dir = UUID.randomUUID().toString();
    Path tmpDir = new Path("/tmp/" + dir);

    try {
      // JOB #1 grouping the occurrences
      Job job = new Job(conf, "density-cube backfill prepare");
      job.setJarByClass(TileCollectorMapper.class); // required to set up MR classpath
      job.getConfiguration().set("mapred.task.timeout", "600000"); // 10 mins
      job.getConfiguration().set("hbase.regionserver.lease.period", "600000");
      job.getConfiguration().set("mapred.compress.map.output", "true");
      job.getConfiguration().set("mapred.output.compress", "true");
      TableMapReduceUtil.initTableMapperJob(conf.get(Backfill.KEY_SOURCE_TABLE), getScanner(conf),
        LocationCollectorMapper.class, OccurrenceWritable.class, IntWritable.class, job);
      job.setCombinerClass(GroupByOccurrenceReducer.class); // Optimize network bandwidth
      job.setReducerClass(GroupByOccurrenceReducer.class);
      job.setNumReduceTasks(conf.getInt(Backfill.KEY_NUM_REDUCERS, Backfill.DEFAULT_NUM_REDUCERS));
      job.setOutputKeyClass(OccurrenceWritable.class);
      job.setOutputValueClass(IntWritable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(job, tmpDir);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Unknown error with job.  Check the logs.");
      }

      // Job #2 to write the cube
      conf.set("mapred.child.java.opts", "-Xmx3G"); // for safety, do not assume the default works
      job = new Job(conf, "density-cube backfill write");
      job.setJarByClass(TileCollectorMapper.class); // required to set up MR classpath
      job.setNumReduceTasks(conf.getInt(Backfill.KEY_NUM_REDUCERS, Backfill.DEFAULT_NUM_REDUCERS));
      SequenceFileInputFormat.setInputPaths(job, tmpDir);
      // This appears to be the only way to get a SequenceFileInputFormat to understand types
      SequenceFileInputFormat<OccurrenceWritable, IntWritable> sequenceInputFormat =
        new SequenceFileInputFormat<OccurrenceWritable, IntWritable>();
      job.setInputFormatClass(sequenceInputFormat.getClass());
      job.setMapperClass(TileCollectorMapper.class);
      job.setMapOutputKeyClass(TileKeyWritable.class);
      job.setMapOutputValueClass(OccurrenceWritable.class);
      // Ensure no bad counts in the cube
      job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
      job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
      job.getConfiguration().set("mapred.compress.map.output", "true");
      job.setReducerClass(CubeWriterReducer.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Unknown error with job.  Check the logs.");
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      // Cleanup by deleting the temporary directory
      FileSystem fs = null;
      try {
        fs = FileSystem.get(tmpDir.toUri(), conf);
        fs.delete(tmpDir, true);
      } finally {
        fs.close();
      }
    }
  }

  private Scan getScanner(Configuration conf) {
    Scan scan = new Scan();
    scan.setCaching(conf.getInt(Backfill.KEY_SCANNER_CACHE, HBaseSourcedBackfill.DEFAULT_SCANNER_CACHE));
    scan.setCacheBlocks(false); // not needed for efficient scanning

    // Optimize the scan by bringing back only what the TileCollectMapper wants
    addFieldToScan(scan, FieldName.I_KINGDOM_ID);
    addFieldToScan(scan, FieldName.I_PHYLUM_ID);
    addFieldToScan(scan, FieldName.I_CLASS_ID);
    addFieldToScan(scan, FieldName.I_ORDER_ID);
    addFieldToScan(scan, FieldName.I_FAMILY_ID);
    addFieldToScan(scan, FieldName.I_GENUS_ID);
    addFieldToScan(scan, FieldName.I_SPECIES_ID);
    addFieldToScan(scan, FieldName.I_NUB_ID);
    addFieldToScan(scan, FieldName.OWNING_ORG_KEY);
    addFieldToScan(scan, FieldName.DATASET_KEY);
    addFieldToScan(scan, FieldName.DATA_RESOURCE_ID);
    addFieldToScan(scan, FieldName.I_ISO_COUNTRY_CODE);
    addFieldToScan(scan, FieldName.HOST_COUNTRY);
    addFieldToScan(scan, FieldName.I_LATITUDE);
    addFieldToScan(scan, FieldName.I_LONGITUDE);
    addFieldToScan(scan, FieldName.I_GEOSPATIAL_ISSUE);
    addFieldToScan(scan, FieldName.I_YEAR);
    addFieldToScan(scan, FieldName.I_BASIS_OF_RECORD);
    addFieldToScan(scan, FieldName.PROTOCOL);
    return scan;
  }
}
