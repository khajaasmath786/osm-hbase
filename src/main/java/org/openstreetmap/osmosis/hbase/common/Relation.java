package org.openstreetmap.osmosis.hbase.common;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;

import java.util.List;

/**
 * Created by willtemperley@gmail.com on 18-Oct-16.
 */
public class Relation extends org.openstreetmap.osmosis.core.domain.v0_6.Relation implements Entity {

    public Relation(org.openstreetmap.osmosis.core.domain.v0_6.Relation relation) {
        super(new CommonEntityData(relation.getId(),relation.getVersion(), relation.getTimestamp(), relation.getUser(), relation.getChangesetId(), relation.getTags()), relation.getMembers());
    }

    public Relation(CommonEntityData entityData, List<RelationMember> members) {
        super(entityData, members);
    }
}
