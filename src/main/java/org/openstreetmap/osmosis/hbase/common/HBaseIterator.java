package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.openstreetmap.osmosis.core.container.v0_6.*;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;

import java.util.Iterator;

/**
 * This is currently only used for tests, to iterate the entire DB
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public class HBaseIterator<T extends Entity> implements ReleasableIterator<EntityContainer> {

    private final Iterator<Result> iterator;
    private final ResultScanner scanner;
    private final EntitySerDe<T> entitySerDe;

    public HBaseIterator(ResultScanner scanner, EntitySerDe<T> entitySerDe) {

        this.iterator = scanner.iterator();
        this.scanner = scanner;
        this.entitySerDe = entitySerDe;
    }


    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public EntityContainer next() {
        Result result = iterator.next();
        Entity entity = entitySerDe.deSerialize(result);

        EntityType type = entity.getType();

        if (type.equals(EntityType.Node)) {
            return new NodeContainer((Node) entity);
        } else if (type.equals(EntityType.Way)) {
            return new WayContainer((Way) entity);
        } else if (type.equals(EntityType.Relation)) {
            return new RelationContainer((Relation) entity);
        } else if (type.equals(EntityType.Bound)) {
            return new BoundContainer((Bound) entity);
        } else {
            throw new RuntimeException("Unknown entity type: " + type);
        }

    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public void release() {
        scanner.close();
    }
}
