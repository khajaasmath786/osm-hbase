package org.openstreetmap.osmosis.hbase.mr.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Created by willtemperley@gmail.com on 11-Mar-16.
 */
public class WayNodeWritable implements Writable {

    private long wayId;

    public long getWayId() {
        return wayId;
    }

    private int ordinal;

    public int getOrdinal() {
        return ordinal;
    }

    public void set(long wayId, int ordinal) {
        this.wayId = wayId;
        this.ordinal = ordinal;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(wayId);
        out.writeInt(ordinal);
    }

    public  void  readFields(DataInput in) throws IOException {
        wayId = in.readLong();
        ordinal = in.readInt();
    }
}
