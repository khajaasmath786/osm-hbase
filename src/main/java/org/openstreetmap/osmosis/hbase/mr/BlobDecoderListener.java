package org.openstreetmap.osmosis.hbase.mr;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;

import java.util.List;

/**
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public class BlobDecoderListener implements PbfBlobDecoderListener {

    public List<EntityContainer> entityContainers;

    @Override
    public void complete(List<EntityContainer> decodedEntities) {
        entityContainers = decodedEntities;
    }

    @Override
    public void error() {
    }

    public List<EntityContainer> getEntityContainers() {
        return entityContainers;
    }
}
