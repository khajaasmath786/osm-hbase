package org.openstreetmap.osmosis.hbase.mr.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openstreetmap.osmosis.hbase.xyz.TileKeyWritable;
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable;
import xyz.mercator.MercatorTile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 14-Mar-17.
 */
public class ImageRegions extends Configured implements Tool {

    public static final String N_HISTOGRAM_BINS = "nHistogramBins";

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("Usage: ImageRegions input-table output-seqfile-path nBins");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01");
        conf.set("hbase.master", "hadoop-m2");

        conf.set(N_HISTOGRAM_BINS, args[2]);

        int res = ToolRunner.run(conf, new ImageRegions(), args);
        System.exit(res);
    }

    public Scan getScan(String tableName) {
        Scan  scan = new Scan();
        //fixme can we have a data-only version?
        scan.addFamily(TileKeyWritable.dataCF);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
        return scan;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        List<Scan> scans = new ArrayList<Scan>();
        scans.add(getScan(args[0]));

        TableMapReduceUtil
                .initTableMapperJob(scans,
                        TileMapper.class,
                        WebTileWritable.class,
                        IntWritable.class,
                        job, true, false);
        TableMapReduceUtil.addDependencyJars(job);

        job.setReducerClass(ImageRegions.TileReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(WebTileWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (job.waitForCompletion(true)) return 0; else return 1;
    }

    public static class TileReducer extends Reducer<WebTileWritable, IntWritable, WebTileWritable, Text> {

        int nBins = -1;

        Text valOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String n = context.getConfiguration().get(N_HISTOGRAM_BINS);
            nBins = Integer.valueOf(n);
        }

        public void reduce(WebTileWritable key, Iterable<IntWritable> values, Reducer<WebTileWritable, IntWritable, WebTileWritable, Text>.Context context) throws IOException, InterruptedException {

            int[] intCounter = new int[nBins];

            MercatorTile tile = new MercatorTile();
            key.getTile(tile);

            for (IntWritable intWritable : values) {
                intCounter[intWritable.get()] += 1;
            }

            String outTextTemplate = "%s:%s";

            for (int i = 0; i < intCounter.length; i++) {

                int count = intCounter[i];
                if (count > 0) {
                    String outText = String.format(outTextTemplate, i, count);
                    valOut.set(outText);
                    context.write(key, valOut);
                }
            }
        }
    }
}
