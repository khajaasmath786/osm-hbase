package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;


/**
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public class WayDao extends EntityDao<Way> {

    public WayDao(Table table) {
        super(table);
    }


    @Override
    public EntitySerDe<Way> getSerDe() {
        return new WaySerDe();
    }

    public void putGeometry(Way way, byte[] lineString) throws IOException {
        byte[] rowKey = EntityDataAccess.getRowKey(way);
        Put put = new Put(rowKey);
        put.add(new KeyValue(rowKey, EntityDataAccess.data, WaySerDe.geom, lineString));
        table.put(put);
    }

}
