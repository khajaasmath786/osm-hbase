package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helps generate
 *
 * //FIXME can't we specialize put?
 *
 * Created by willtemperley@gmail.com on 26-Jul-16.
 */
public class CellGenerator {

    byte[] columnFamily;

    public CellGenerator(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public KeyValue getKeyValue(byte[] rowKey, byte[] column, byte[] value) {
        return new KeyValue(rowKey, columnFamily, column, value);
    }

    public KeyValue getKeyValue(byte[] rowKey, byte[] column, String value) {
        return new KeyValue(rowKey, columnFamily, column, Bytes.toBytes(value));
    }

    public KeyValue getKeyValue(byte[] rowKey, byte[] column, int value) {
        return new KeyValue(rowKey, columnFamily, column, Bytes.toBytes(value));
    }

    public KeyValue getKeyValue(byte[] rowKey, byte[] column, double value) {
        return new KeyValue(rowKey, columnFamily, column, Bytes.toBytes(value));
    }

    public KeyValue getKeyValue(byte[] rowKey, byte[] column, long value) {
        return new KeyValue(rowKey, columnFamily, column, Bytes.toBytes(value));
    }

}
