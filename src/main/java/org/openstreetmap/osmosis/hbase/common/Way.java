package org.openstreetmap.osmosis.hbase.common;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Polyline;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.List;

/**
 * Created by willtemperley@gmail.com on 18-Oct-16.
 */
public class Way extends org.openstreetmap.osmosis.core.domain.v0_6.Way implements Entity {

    /*
     * To avoid null geometries an empty one is always available
     */
    static Geometry emptyGeom;

    static {
        emptyGeom = new Polyline();
    }

    public Way(org.openstreetmap.osmosis.core.domain.v0_6.Way way) {
        super(new CommonEntityData(way.getId(), way.getVersion(), way.getTimestamp(), way.getUser(), way.getChangesetId(), way.getTags()), way.getWayNodes());
    }

    public Way(CommonEntityData entityData, List<WayNode> wayNodes, Geometry geometry) {
        super(entityData, wayNodes);
        this.geometry = geometry;
    }

    private Geometry geometry;

    public Geometry getGeometry() {
        return geometry;
    }

    public void setGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

}
