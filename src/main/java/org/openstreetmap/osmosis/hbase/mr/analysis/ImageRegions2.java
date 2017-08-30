package org.openstreetmap.osmosis.hbase.mr.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.resources.Arguments;
import org.openstreetmap.osmosis.hbase.xyz.TileKeyWritable;
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable;
import xyz.mercator.MercatorTile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 14-Mar-17.
 */
public class ImageRegions2 extends Configured implements Tool {

    public static final String N_HISTOGRAM_BINS = "nHistogramBins";

    public static void main(String[] args) throws Exception {


        if (args.length != 6) {
            System.out.println("Usage: ImageRegions2 -i input-table -o output-table -n nBins");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01");
        conf.set("hbase.master", "hadoop-m2");


        int res = ToolRunner.run(conf, new ImageRegions2(), args);
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

        Arguments processedArgs = new Arguments(args);
        String inTable  = processedArgs.getRequiredString("-i");
        String outTable = processedArgs.getRequiredString("-o");
        int nBins = processedArgs.getRequiredInteger("-n");

        Configuration conf = getConf();
        conf.set(N_HISTOGRAM_BINS, String.valueOf(nBins));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        List<Scan> scans = new ArrayList<Scan>();
        scans.add(getScan(inTable));

        TableMapReduceUtil
                .initTableMapperJob(scans,
                        TileMapper.class,
                        WebTileWritable.class,
                        IntWritable.class,
                        job, true, false);
        TableMapReduceUtil.addDependencyJars(job);

        //no need to specify key val outputs
        TableMapReduceUtil.initTableReducerJob(outTable, TileTableReducer.class, job);

//        job.setReducerClass(ImageRegions2.TileReducer.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setOutputKeyClass(WebTileWritable.class);
//        job.setOutputValueClass(Text.class);

//        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (job.waitForCompletion(true)) return 0; else return 1;
    }

}
