package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

import java.util.List;

/**
 * encode/decode a Node
 *
 * Created by willtemperley@gmail.com on 18-Jul-16.
 */
public class NodeSerDe extends EntitySerDe<Node> {

    private static byte[] latitude = Bytes.toBytes("lat");

    private static byte[] longitude = Bytes.toBytes("lon");

    @Override
    public int getEntityType() {
        return EntityType.Node.ordinal();
    }

    @Override
    public void encode(byte[] rowKey, Node entity, List<Cell> keyValues) {

        Cell lat = getDataCellGenerator().getKeyValue(rowKey, latitude, entity.getLatitude());
        Cell lon = getDataCellGenerator().getKeyValue(rowKey, longitude, entity.getLongitude());

        keyValues.add(lat);
        keyValues.add(lon);
    }

    @Override
    public Node constructEntity(Result result, CommonEntityData commonEntityData) {

        Double lat = getDouble(latitude, result);
        Double lon = getDouble(longitude, result);

        return new Node(commonEntityData, lat, lon);
    }
}
