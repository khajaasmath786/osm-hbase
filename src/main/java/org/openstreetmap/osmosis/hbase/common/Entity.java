package org.openstreetmap.osmosis.hbase.common;

import org.openstreetmap.osmosis.core.domain.common.TimestampContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Date;

/**
 * Created by willtemperley@gmail.com on 18-Oct-16.
 */
public interface Entity {

    long getId();

    int getVersion();

    Date getTimestamp();

    TimestampContainer getTimestampContainer();

    OsmUser getUser();

    long getChangesetId();

    Collection<Tag> getTags();

    EntityType getType();
}
