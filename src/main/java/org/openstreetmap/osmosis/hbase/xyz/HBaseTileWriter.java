package org.openstreetmap.osmosis.hbase.xyz;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.hbase.ConfigurationFactory;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import xyz.wgs84.TileKey;

import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 13-Mar-17.
 */
public class HBaseTileWriter implements TileWriter {


    private final Table table;

    public HBaseTileWriter(Table table) {

        this.table = table;
//        try {
//            // = ConnectionFactory.createConnection(ConfigurationFactory.getConfiguration());
//            this.table = tableFactory.getTable();
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    TileKeyWritable tileKeyWritable = new TileKeyWritable();
    ImageTileWritable imageTileWritable = new ImageTileWritable();


    private int i = 0;
    @Override
    public void append(TileKey key, int[] image) throws IOException {

        tileKeyWritable.setTile(key, ++i);
        imageTileWritable.setImage(image);

        Put put = new Put(tileKeyWritable.get());
        put.addColumn(TileKeyWritable.dataCF, TileKeyWritable.imageC, imageTileWritable.get());

        table.put(put);
    }

    @Override
    public void close() throws IOException {
        table.close();
    }
}
