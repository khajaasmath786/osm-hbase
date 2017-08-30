package org.openstreetmap.osmosis.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.internal.matchers.ArrayEquals;

import java.io.IOException;

/**
 * Writes
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class CellReducer extends Reducer<ImmutableBytesWritable, Cell, ImmutableBytesWritable, Put> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Cell> values, Context context) throws IOException, InterruptedException {

        Put put = new Put(key.get());
        for (Cell value : values) {
            put.add(value);
        }

        context.write(key, put);

    }

}
