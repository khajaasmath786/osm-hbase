package org.openstreetmap.osmosis.hbase.mr.analysis;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.openstreetmap.osmosis.hbase.xyz.ImageIterable;
import org.openstreetmap.osmosis.hbase.xyz.ImageIterableByte;
import org.openstreetmap.osmosis.hbase.xyz.TileKeyWritable;
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable;
import xyz.mercator.MercatorTile;
import xyz.mercator.MercatorTileCalculator;
import xyz.wgs84.TileKey;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by willtemperley@gmail.com on 20-Mar-17.
 */
public class TileMapper extends TableMapper<WebTileWritable, IntWritable> {

    WebTileWritable tileWritable = new WebTileWritable();
    IntWritable intWritable = new IntWritable();

    MercatorTileCalculator mercatorTileCalculator = new MercatorTileCalculator();

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        TileKeyWritable tileKeyWritable = new TileKeyWritable();
        tileKeyWritable.set(key.get());

        TileKey tileKey = new TileKey();
        tileKeyWritable.readTile(tileKey);

        byte[] val = value.getValue(TileKeyWritable.dataCF, TileKeyWritable.imageC);

        ByteBuffer buffer = ByteBuffer.wrap(val);

        MercatorTile theTile = new MercatorTile();

        ImageIterable imageIterable = new ImageIterableByte(tileKey, buffer);
        while (imageIterable.hasNext()) {
            int v = imageIterable.getValue();
            intWritable.set(v);
            double[] coord = imageIterable.getCoordinate();
            mercatorTileCalculator.tileForCoordinate(coord[0], coord[1], 14, theTile);
            tileWritable.setTile(theTile);

            context.write(tileWritable, intWritable);
        }
    }
}
