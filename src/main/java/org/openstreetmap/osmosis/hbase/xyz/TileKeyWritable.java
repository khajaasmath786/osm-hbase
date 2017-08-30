package org.openstreetmap.osmosis.hbase.xyz;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import xyz.wgs84.TileKey;

import java.nio.ByteBuffer;

/**
 * fixme Can we move some of the serialization into xyz?
 *
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TileKeyWritable extends ImmutableBytesWritable {

    public static byte[] dataCF = Bytes.toBytes("d");
    public static byte[] imageC = Bytes.toBytes("i");

    private final int bufferSize =  4 + (4 * 8) + (3 * 4);


    public void setTile(TileKey tileKey, int salt) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[bufferSize]);

        //reversed bytes
        byte[] bytes = Bytes.toBytes(salt);
        buf.put(bytes[3]);
        buf.put(bytes[2]);
        buf.put(bytes[1]);
        buf.put(bytes[0]);

        buf.putDouble(tileKey.getOriginX());
        buf.putDouble(tileKey.getOriginY());
        buf.putDouble(tileKey.getPixelSizeX());
        buf.putDouble(tileKey.getPixelSizeY());
        buf.putInt(tileKey.getWidth());
        buf.putInt(tileKey.getHeight());
        buf.putInt(tileKey.getProj());

        byte[] array = buf.array();
        this.set(array);
    }

    /**
     * @param tileKey
     *
     * Object reuse encouraged
     */
    public void readTile(TileKey tileKey) {

        ByteBuffer arr = ByteBuffer.wrap(this.get());

        arr.getInt(); //Salt, thrown away

        tileKey.setOrigin(arr.getDouble(), arr.getDouble());
        tileKey.setPixelSize(arr.getDouble(), arr.getDouble());
        tileKey.setDimensions(arr.getInt(), arr.getInt());
        tileKey.setProj(arr.getInt());
    }

}
