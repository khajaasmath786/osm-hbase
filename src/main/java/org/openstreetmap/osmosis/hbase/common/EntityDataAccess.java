package org.openstreetmap.osmosis.hbase.common;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants and basic methods for OSM data access
 *
 * Created by willtemperley@gmail.com on 31-Aug-16.
 */
public class EntityDataAccess {

    public static byte[] tags = Bytes.toBytes("t");
    public static byte[] data = Bytes.toBytes("d");

    protected static byte[] version = Bytes.toBytes("v");
    protected static byte[] timestamp = Bytes.toBytes("ts");
    protected static byte[] changeset = Bytes.toBytes("cs");
    protected static byte[] uid = Bytes.toBytes("uid");
    protected static byte[] uname = Bytes.toBytes("uname");
    public static byte[] entitytype = Bytes.toBytes("et");

    public static long getId(byte[] rowKey) {
        ArrayUtils.reverse(rowKey);
        return Longs.fromByteArray(rowKey);
    }

    public static byte[] getRowKey(Entity entity) {
        long entityId = entity.getId();
        return getRowKey(entityId);
    }

    public static byte[] getRowKey(long entityId) {
        byte[] bytes = Bytes.toBytes(entityId);
        ArrayUtils.reverse(bytes);
        return bytes;
    }

    protected static String getString(byte[] column, Result result) {
        return Bytes.toString(result.getValue(data, column));
    }

    protected static long getLong(byte[] column, Result result) {
        return Bytes.toLong(result.getValue(data, column));
    }

    protected static int getInt(byte[] column, Result result) {
        return Bytes.toInt(result.getValue(data, column));
    }

    protected static double getDouble(byte[] column, Result result) {
        return Bytes.toDouble(result.getValue(data, column));
    }

}
