package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;

/**
 * Node data access
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public class NodeDao extends EntityDao<Node> {

    public NodeDao(Table ways) {
        super(ways);
    }

    @Override
    public EntitySerDe<Node> getSerDe() {
        return new NodeSerDe();
    }

}
