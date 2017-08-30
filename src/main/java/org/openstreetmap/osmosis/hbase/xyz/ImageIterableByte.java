package org.openstreetmap.osmosis.hbase.xyz;

import xyz.wgs84.TileKey;

import java.nio.ByteBuffer;

/**
 * Created by willtemperley@gmail.com on 15-Mar-17.
 */
public class ImageIterableByte extends ImageIterable {

    private final ByteBuffer byteBuffer;

    public ImageIterableByte(TileKey tileKey, ByteBuffer byteBuffer) {
        super(tileKey);
        this.byteBuffer = byteBuffer;
    }

    @Override
    public boolean hasNext() {
        return byteBuffer.hasRemaining();
    }

    @Override
    int get() {
        return byteBuffer.getInt();
    }
}
