package org.openstreetmap.osmosis.hbase.mr;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.hbase.common.Relation;
import org.openstreetmap.osmosis.hbase.common.RelationSerDe;
import org.openstreetmap.osmosis.hbase.mr.OsmEntityMapper;

/**
 * Maps relations to hbase key-values
 *
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class RelationMapper extends OsmEntityMapper<Relation> {
    public RelationMapper() {
        super(EntityType.Relation, new RelationSerDe());
    }

    @Override
    Relation getEntity(Entity entity) {
        return new Relation((org.openstreetmap.osmosis.core.domain.v0_6.Relation) entity);
    }
}
