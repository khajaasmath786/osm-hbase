package org.openstreetmap.osmosis.hbase.mr;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.hbase.common.Way;
import org.openstreetmap.osmosis.hbase.common.WaySerDe;

/**
 * Maps tableWays to hbase key-values
 *
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class WayMapper extends OsmEntityMapper<Way> {

    public WayMapper() {
        super(EntityType.Way, new WaySerDe());
    }

    @Override
    Way getEntity(Entity entity) {
        return new Way((org.openstreetmap.osmosis.core.domain.v0_6.Way) entity);
    }
}
