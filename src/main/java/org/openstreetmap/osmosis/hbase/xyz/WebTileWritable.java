package org.openstreetmap.osmosis.hbase.xyz;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import xyz.Tile;
import xyz.TileKey;
import xyz.tms.TmsTile;

import java.nio.ByteBuffer;

/**
 * Created by willtemperley@gmail.com on 14-Mar-17.
 */
public class WebTileWritable extends ImmutableBytesWritable implements TileKey {

    private final int bufferSize = 12;

    public void setTile(Tile tmsTile) {

        ByteBuffer buf = ByteBuffer.wrap(new byte[bufferSize]);

        buf.putInt(tmsTile.getX());
        buf.putInt(tmsTile.getY());
        buf.putInt(tmsTile.getZ());

        byte[] array = buf.array();
        this.set(array);
    }

    @Override
    public void getTile(Tile tile) {

        byte[] bytes = get();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        tile.setX(buf.getInt());
        tile.setY(buf.getInt());
        tile.setZ(buf.getInt());

    }
}
