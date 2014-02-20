package org.gbif.metrics.cube.index.country.backfill;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.index.common.Combiner;
import org.gbif.metrics.cube.util.Scans;

import java.io.IOException;

import com.urbanairship.datacube.backfill.HBaseBackfillCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * The callback used from the backfill process to spawn the job to write the new
 * data in the cube. This is a simple 1 pass scan of the occurrence data which
 * writes the cube.
 */
class BackfillCallback implements HBaseBackfillCallback {

  @Override
  public void backfillInto(Configuration conf, byte[] table, byte[] cf, long snapshotFinishMs) throws IOException {
    conf = HBaseConfiguration.create(conf);

    try {
      Job job = new Job(conf, "Dataset by country cube writer");
      job.setJarByClass(TableReaderMapper.class); // required to set up MR classpath
      // These are set high, as any failure means inaccurate cube counts
      job.getConfiguration().set("mapred.task.timeout", "1800000"); // 30 mins
      job.getConfiguration().set("hbase.regionserver.lease.period", "1800000");
      // We are doing a huge amount of very tiny PUTs so reduce the buffer, as
      // the default of 2MB means HBase
      // is being asked to process huge numbers of rows, and tasks start to
      // timeout. These will likely hit
      // 1 region if the cube is initially empty.
      job.getConfiguration().set("hbase.client.write.buffer", "262144"); // 256k

      TableMapReduceUtil.initTableMapperJob(conf.get(HBaseSourcedBackfill.KEY_SOURCE_TABLE), getScanner(conf),
        TableReaderMapper.class, Key.class, IntWritable.class, job);
      job.setCombinerClass(Combiner.class); // to reduce transfer, combine on
                                            // the map emission
      job.setReducerClass(CubeWriterReducer.class);
      job.setNumReduceTasks(conf.getInt(HBaseSourcedBackfill.KEY_NUM_REDUCERS,
        HBaseSourcedBackfill.DEFAULT_NUM_REDUCERS));
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Unknown error with job.  Check the logs.");
      }

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private Scan getScanner(Configuration conf) {
    Scan scan = new Scan();
    scan.setCaching(conf.getInt(HBaseSourcedBackfill.KEY_SCANNER_CACHE, HBaseSourcedBackfill.DEFAULT_SCANNER_CACHE));
    scan.setCacheBlocks(false); // not needed for efficient scanning
    // Optimize the scan by bringing back only what the TableReaderMapper wants
    Scans.addTerm(scan, DwcTerm.countryCode);
    Scans.addTerm(scan, GbifTerm.datasetKey);
    return scan;
  }
}
