package org.openstreetmap.osmosis.hbase.mr;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess;
import org.openstreetmap.osmosis.hbase.common.NodeSerDe;
import org.openstreetmap.osmosis.hbase.common.WaySerDe;
import org.openstreetmap.osmosis.hbase.mr.writable.CoordStructWrapper;
import org.openstreetmap.osmosis.hbase.mr.writable.NodeWritable;
import org.openstreetmap.osmosis.hbase.mr.writable.OsmEntityWritable;
import org.openstreetmap.osmosis.hbase.mr.writable.WayNodeWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 29-Sept-16.
 *
 * Essentially an update to the ways table, adds in denormalized way data
 * This isn't much different to having a geometry in postgis really.
 *
 */
public class WayTableLoader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01");
        conf.set("hbase.master", "hadoop-m2");
        int res = ToolRunner.run(conf, new WayTableLoader(), args);
        System.exit(res);
    }

    public Scan getScan(String tableName) {

        Scan  scan = new Scan();
        scan.addFamily(EntityDataAccess.data);
        //fixme can we have a data-only version?
        scan.addFamily(EntityDataAccess.tags);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
        return scan;
    }

    String tableWays = "ways";
    String tableNodes = "nodes";

    @Override
    public int run(String[] args) throws Exception {


        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(this.getClass());

        List<Scan> scans = new ArrayList<Scan>();
        scans.add(getScan(tableWays));
        scans.add(getScan(tableNodes));

        TableMapReduceUtil
                .initTableMapperJob(scans,
                        SiteGridMapper.class,
                        ImmutableBytesWritable.class,
                        Result.class,
                        job, true, false);
        TableMapReduceUtil.addDependencyJars(job);

        //Reduces
        TableMapReduceUtil.initTableReducerJob(tableWays, SiteGridReducer.class, job);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(OsmEntityWritable.class);

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }

    public static class SiteGridMapper extends TableMapper<ImmutableBytesWritable, OsmEntityWritable> {

        WaySerDe waySerDe = new WaySerDe();
        NodeSerDe nodeSerDe = new NodeSerDe();

        ImmutableBytesWritable nodeIdWritable = new ImmutableBytesWritable();
        WayNodeWritable wayNodeWritable = new WayNodeWritable();
        NodeWritable nodeWritable = new NodeWritable();
        OsmEntityWritable entityWritable = new OsmEntityWritable();

        /**
         * To help ensure the entity is set
         *
         * @param nodeId the node to join
         * @param writable an OsmWritable
         * @param context MR context
         * @throws IOException standard
         * @throws InterruptedException standard
         */
        private void write(long nodeId, Writable writable, Context context) throws IOException, InterruptedException {
            nodeIdWritable.set(Bytes.toBytes(nodeId));
            entityWritable.set(writable);
            context.write(nodeIdWritable, entityWritable);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

            /*
            Get the entity type
             */
            int entityTypeOrdinal = Bytes.toInt(result.getValue(EntityDataAccess.data, EntityDataAccess.entitytype));
            EntityType entityType = EntityType.values()[entityTypeOrdinal];

            if (entityType.equals(EntityType.Way)) {
                Way way = waySerDe.deSerialize(result);
                List<WayNode> wayNodes = way.getWayNodes();
                for (int i = 0; i < wayNodes.size(); i++) {
                    long nodeId = wayNodes.get(i).getNodeId();
                    wayNodeWritable.set(nodeId, i);
                    write(nodeId, wayNodeWritable, context);
                }
            } else if (entityType.equals(EntityType.Node)) {
                Node node = nodeSerDe.deSerialize(result);
                long nodeId = node.getId();
                nodeWritable.set(node.getLongitude(), node.getLatitude());
                write(nodeId, nodeWritable, context);
            }
        }
    }

    public static class SiteGridReducer extends TableReducer<ImmutableBytesWritable, OsmEntityWritable, ImmutableBytesWritable> {

        CoordStructWrapper coordStructWrapper = new CoordStructWrapper();
        ImmutableBytesWritable wayKey = new ImmutableBytesWritable();

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<OsmEntityWritable> values, Context context) throws IOException, InterruptedException {

            NodeWritable nodeWritable = null;
            List<WayNodeWritable> wayNodeWritables = new ArrayList<WayNodeWritable>();

            for (OsmEntityWritable value : values) {
                Writable writable = value.get();
                if (writable instanceof NodeWritable) {
                    nodeWritable = (NodeWritable) writable;
                } else if (writable instanceof WayNodeWritable) {
                    wayNodeWritables.add((WayNodeWritable) writable);
                }
            }
            if (nodeWritable == null) {
                throw new RuntimeException("No node was found for this waynode.");
            }

            for (WayNodeWritable wayNodeWritable : wayNodeWritables) {
                long wayId = wayNodeWritable.getWayId();
                int ordinal = wayNodeWritable.getOrdinal();
                byte[] rowKey = EntityDataAccess.getRowKey(wayId);
                wayKey.set(rowKey);

                byte[] coordStruct = coordStructWrapper.encode(nodeWritable);

                Put put = new Put(wayKey.get());
                put.add(new KeyValue(wayKey.get(), EntityDataAccess.data, coordStructWrapper.getWayNodeColumn(ordinal), coordStruct));
                context.write(wayKey, put);
            }
        }
    }

}
