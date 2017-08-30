package org.openstreetmap.osmosis.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openstreetmap.osmosis.hbase.ConfigurationFactory;

/**
 *
 * Created by willtemperley@gmail.com on 26-Jul-16.
 */
public class TableLoader extends Configured implements Tool{

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("usage: table inpath outpath");
            return;
        }

        Configuration conf = ConfigurationFactory.getConfiguration();

        int res = ToolRunner.run(conf, new TableLoader(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        String tableName = args[0];

        Configuration conf = getConf();

        Class<? extends Mapper> mapperClass;// = OsmEntityMapper.class;
//        EntityType entityType = en

        if (tableName.equals("nodes")) {
            mapperClass = NodeMapper.class;
        } else if (tableName.equals("ways")) {
            mapperClass = WayMapper.class;
        } else if (tableName.equals("relations")) {
            mapperClass = RelationMapper.class;
        } else {
            throw new RuntimeException("the table " + tableName + " is unknown.");
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(this.getClass());

        job.setMapperClass(mapperClass);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);


        HTable outTable = new HTable(conf, tableName);
        FileInputFormat.addInputPath(job, new Path(args[1]));

        Path outPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outPath);

        HFileOutputFormat2.configureIncrementalLoad(job, outTable, outTable.getRegionLocator());

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }


}
