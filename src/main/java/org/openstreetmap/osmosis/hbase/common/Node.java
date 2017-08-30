package org.openstreetmap.osmosis.hbase.common;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;

/**
 * Created by willtemperley@gmail.com on 18-Oct-16.
 */
public class Node extends org.openstreetmap.osmosis.core.domain.v0_6.Node implements Entity {

    public Node(org.openstreetmap.osmosis.core.domain.v0_6.Node node) {
        super(new CommonEntityData(node.getId(),node.getVersion(), node.getTimestamp(), node.getUser(), node.getChangesetId(), node.getTags()), node.getLatitude(), node.getLongitude());
    }

    public Node(CommonEntityData entityData, double latitude, double longitude) {
        super(entityData, latitude, longitude);
    }
}
