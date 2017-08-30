package org.openstreetmap.osmosis.hbase.mr.writable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.types.RawDouble;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

/**
 * Created by willtemperley@gmail.com on 29-Sep-16.
 */
public class CoordStructWrapper {

    private final PositionedByteRange positionedByteRange = new SimplePositionedMutableByteRange();
    private final Struct struct;
    byte[] nodeColPrefix = Bytes.toBytes("n");

    public CoordStructWrapper() {

        /*
        Seems that strings have to be in the right-most position as they are variable length
        Can't have two strings
         */
        StructBuilder structBuilder = new StructBuilder();
        structBuilder.add(new RawDouble());//member id
        structBuilder.add(new RawDouble());//member type
        struct = structBuilder.toStruct();

    }

    public byte[] getWayNodeColumn(int i) {
        return Bytes.add(nodeColPrefix, Bytes.toBytes(i));
    }

    public byte[] encode(NodeWritable nodeWritable) {

        positionedByteRange.set(new byte[16]);
        Object[] val = {nodeWritable.getX(), nodeWritable.getY()};
        struct.encode(positionedByteRange, val);
        return positionedByteRange.getBytes();
    }

}
