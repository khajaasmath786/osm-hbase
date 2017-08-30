// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.esri.core.geometry.*;
import com.google.inject.Inject;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.hbase.common.*;


/**
 * A change sink writing to hbase tables. This aims to be suitable for
 * running at regular intervals with database overhead proportional to changeset
 * size.
 * 
 * @author willtemperley@gmail.com
 */
public class HBaseWriter implements ChangeSink, Sink {

    private final TableFactory tableFactory;
    private final WayLineStringBuilder wayLineStringBuilder;

    private boolean keepInvalidWays;
    private final RelationDao relationDao;
    private final NodeDao nodeDao;
    private final WayDao wayDao;


    private static class WayLineStringBuilder {

        private final NodeDao nodeDao;
        private final OperatorExportToWkb wkbWriter = OperatorExportToWkb.local();

        public WayLineStringBuilder(NodeDao nodeDao) {
            this.nodeDao = nodeDao;
        }

        private List<Node> getNodes(Way way) {
            List<WayNode> wayNodes = way.getWayNodes();
            long[] longs = new long[wayNodes.size()];
            for (int i = 0; i < wayNodes.size(); i++) {
                longs[i] = wayNodes.get(i).getNodeId();
            }
            return nodeDao.get(longs);
        }

        public Geometry getLineString(Way way) {

            List<Node> nodes = getNodes(way);

            if (nodes.size() < 2) {
                return new Polyline();
            }

            Polyline polyline = new Polyline();

            for (int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                if (i == 0) {
                    polyline.startPath(node.getLongitude(), node.getLatitude());
                } else {
                    polyline.lineTo(node.getLongitude(), node.getLatitude());
                }
            }

            return polyline;
        }

        public ByteBuffer getLineStringAsWKB(Way way) {
            Geometry lineString = getLineString(way);
            return wkbWriter.execute(0, lineString, null);
        }

    }

    @Inject
	public HBaseWriter(TableFactory tableFactory) throws IOException {

	    this.tableFactory = tableFactory;

	    Table ways = tableFactory.getTable("ways");
        Table nodes = tableFactory.getTable("nodes");
        Table relations = tableFactory.getTable("relations");

        wayDao = new WayDao(ways);
        nodeDao = new NodeDao(nodes);
        relationDao = new RelationDao(relations);

        wayLineStringBuilder = new WayLineStringBuilder(nodeDao);
	}
	

    /**
     * {@inheritDoc}
     */
    public void initialize(Map<String, Object> metaData) {
		// Do nothing.
    }


	/**
	 * {@inheritDoc}
	 */
	public void process(ChangeContainer change) {

		ChangeAction action = change.getAction();
        EntityContainer entityContainer = change.getEntityContainer();
        Entity entity = entityContainer.getEntity();

        process(entity, action);
	}


	private void process(Entity entity, ChangeAction action) {

        List<Long> wayIds = new ArrayList<Long>();

        //bounds aren't processed
        EntityType entityType = entity.getType();

        if (entityType.equals(EntityType.Node)) {
            nodeDao.process(new Node((org.openstreetmap.osmosis.core.domain.v0_6.Node) entity), action);
        } else if (entityType.equals(EntityType.Way)) {
            wayDao.process(new Way((org.openstreetmap.osmosis.core.domain.v0_6.Way) entity), action);

            /*
            for post-processing
             */
            if (action.equals(ChangeAction.Create) || action.equals(ChangeAction.Modify)) {
                wayIds.add(entity.getId());
            }
        } else if (entityType.equals(EntityType.Relation)) {
            relationDao.process(new Relation((org.openstreetmap.osmosis.core.domain.v0_6.Relation) entity), action);
        }

        /*
         * Ways need to be processed again to add linestrings
         */
        for (Long wayId : wayIds) {
            Way way = wayDao.get(wayId);
            ByteBuffer lineStringAsWKB = wayLineStringBuilder.getLineStringAsWKB(way);
            try {

                byte[] dst = new byte[lineStringAsWKB.remaining()];
                lineStringAsWKB.get(dst);
                wayDao.putGeometry(way, dst);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }


    @Override
    public void process(EntityContainer entityContainer) {

        Entity entity = entityContainer.getEntity();

        process(entity, ChangeAction.Create);
    }

	
	/**
	 * {@inheritDoc}
	 */
	public void complete() {

	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public void release() {

        tableFactory.close();

	}

    public void keepInvalidWays(boolean keepInvalidWays) {
        this.keepInvalidWays = keepInvalidWays;
    }
}
