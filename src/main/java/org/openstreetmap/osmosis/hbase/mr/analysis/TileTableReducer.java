package org.openstreetmap.osmosis.hbase.mr.analysis;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.openstreetmap.osmosis.hbase.xyz.WebTileWritable;
import xyz.mercator.MercatorTile;

import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 20-Mar-17.
 */
public class TileTableReducer extends TableReducer<WebTileWritable, IntWritable, WebTileWritable> {

    int nBins = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String n = context.getConfiguration().get(ImageRegions.N_HISTOGRAM_BINS);
        nBins = Integer.valueOf(n);
    }

    @Override
    protected void reduce(WebTileWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int[] intCounter = new int[nBins];

        MercatorTile tile = new MercatorTile();
        key.getTile(tile);

        for (IntWritable intWritable : values) {
            intCounter[intWritable.get()] += 1;
        }

        Put put = new Put(key.get());
        for (int i = 0; i < intCounter.length; i++) {

            int count = intCounter[i];
            if (count > 0) {
                put.addColumn(TileStatsSerDe.cfD, Bytes.toBytes(i), Bytes.toBytes(count));
            }
        }

        context.write(key, put);
    }
}
