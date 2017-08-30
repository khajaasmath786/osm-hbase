package org.openstreetmap.osmosis.hbase.mr.analysis;

import com.esri.core.geometry.*;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess;
import org.openstreetmap.osmosis.hbase.common.WaySerDe;
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable;
import xyz.tms.TmsTile;
import xyz.tms.TmsTileCalculator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 29-Sept-16.
 *
 * Essentially an update to the ways table, adds in denormalized way data
 * This isn't much different to having a geometry in postgis really.
 *
 */
public class RoadlessMap extends Configured implements Tool{

    String tableWays = "ways";
    String tableNodes = "nodes";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop-m2,hadoop-m1,hadoop-01");
        conf.set("hbase.master", "hadoop-m2");
        int res = ToolRunner.run(conf, new RoadlessMap(), args);
        System.exit(res);
    }

    public Scan getScan(String ways) {
        Scan  scan = new Scan();
        scan.addFamily(EntityDataAccess.data);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(ways));
        return scan;
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(this.getClass());

        List<Scan> scans = new ArrayList<Scan>();
        scans.add(getScan(tableWays));

        TableMapReduceUtil
                .initTableMapperJob(scans,
                        WayTableMapper.class,
                        ImmutableBytesWritable.class,
                        Result.class,
                        job, true, false);
        TableMapReduceUtil.addDependencyJars(job);

        //Reduces
        TableMapReduceUtil.initTableReducerJob(tableWays, BufferReducer.class, job);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }

    public static class WayTableMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        ImmutableBytesWritable idWritable = new ImmutableBytesWritable();
        ImmutableBytesWritable geomWritable = new ImmutableBytesWritable();

        TmsTileCalculator tileCalculator = new TmsTileCalculator();

        OperatorIntersects operatorIntersects = OperatorIntersects.local();
        final PositionedByteRange positionedByteRange = new SimplePositionedMutableByteRange();

        final WebTileWritable webTileWritable = new WebTileWritable();
        OperatorImportFromWkb fromWkb = OperatorImportFromWkb.local();

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

            /*
            Get the entity type
             */
            byte[] value = result.getValue(EntityDataAccess.data, WaySerDe.geom);

            //Todo: in the future, there may be variable geometry types;;
            Geometry geometry = fromWkb.execute(0, Geometry.Type.Polyline, ByteBuffer.wrap(value), null);

            Envelope2D env = new Envelope2D();
            geometry.queryEnvelope2D(env);

            SpatialReference spatialRef = SpatialReference.create(4326);

            List<TmsTile> tiles = tileCalculator.tilesForEnvelope(env, 9);

            for (TmsTile tile : tiles) {
                Polygon envelopeAsPolygon = tile.getEnvelopeAsPolygon();
                boolean tileIntersects = operatorIntersects.execute(envelopeAsPolygon, geometry, spatialRef, null);
                if(tileIntersects) {

                    webTileWritable.setTile(tile);
                    geomWritable.set(value);
                    context.write(webTileWritable, geomWritable);
                }
            }

        }
    }

    /**
     * Should take a bunch of geometries, buffer then rasterize them.
     */
    public static class BufferReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        GeomIO<Polyline> geomIO = new GeomIO<Polyline>(Geometry.Type.Polyline);
        SpatialReference spatialRef = SpatialReference.create(4326);

        WebTileWritable tileWritable = new WebTileWritable();

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {

            TmsTile tile = new TmsTile();
            tileWritable.getTile(tile);
            //fixme
            TileRasterizer tileRasterizer = new TileRasterizer(tile, new TileScanCallback() {
                @Override
                public byte[] getImage() {
                    return new byte[0];
                }

                @Override
                public int getWidth() {
                    return 0;
                }

                @Override
                public int getHeight() {
                    return 0;
                }

                @Override
                public byte[] getBitSet() {
                    return new byte[0];
                }

                @Override
                public void drawScan(int[] scans, int scanCount3) {

                }
            });

            /*
            Iterate all geometries,
             */
            for (ImmutableBytesWritable value : values) {

                Polyline geometry = geomIO.getGeometry(value.get());
                Geometry outputGeom = OperatorBuffer.local().execute(geometry, spatialRef, 0.005, null);
                tileRasterizer.rasterizePolygon((Polygon) outputGeom);
            }

            byte[] image = tileRasterizer.getImage();
            Put put = createTileImagePut(key, image);

            context.write(key, put);

//            writeDebugTile(tile, image);

        }

        /**
         *
         * @param key tile key
         * @param imgData png image
         * @return a kv of params
         * @throws IOException ...
         */
        private Put createTileImagePut(ImmutableBytesWritable key, byte[] imgData) throws IOException {
            KeyValue kv = new KeyValue(key.get(), EntityDataAccess.data, imgData);
            Put put = new Put(key.get());
            put.add(kv);
            return put;
        }

        private void writeDebugTile(TmsTile tile, byte[] bytes) throws IOException {
            File f = new File("e:/tmp/ras/" + tile.toString() + ".png");
            FileOutputStream fileOutputStream = new FileOutputStream(f);
            for (byte aByte : bytes) {
                fileOutputStream.write(aByte);
            }
        }
    }

}
