package org.openstreetmap.osmosis.hbase.reader;

import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;

/**
 * We only ever seem to need one Bound but the datasetcontext needs an iterator
 *
 * Created by willtemperley@gmail.com on 19-Jul-16.
 */
public class BoundsIterator implements ReleasableIterator<EntityContainer> {


    private final Bound bound;
    private boolean hasNext = true;

    public BoundsIterator(Bound bound) {
       this.bound = bound;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public BoundContainer next() {
        hasNext = false;
        return new BoundContainer(bound);
    }

    @Override
    public void remove() {

    }

    @Override
    public void release() {

    }
}
