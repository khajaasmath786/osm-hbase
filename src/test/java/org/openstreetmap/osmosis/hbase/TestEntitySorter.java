package org.openstreetmap.osmosis.hbase;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;
import org.openstreetmap.osmosis.core.sort.v0_6.EntityByTypeThenIdComparator;
import org.openstreetmap.osmosis.core.sort.v0_6.EntityContainerComparator;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.core.task.v0_6.Source;

import java.util.*;

/**
 * Sorts entities by type then id, for creating comparable test files.
 *
 * Created by willtemperley@gmail.com on 19-Jul-16.
 */
public class TestEntitySorter implements Source, Sink {
    private final ArrayList<EntityContainer> entities;
    private Sink sink;

    @Override
    public void setSink(Sink sink) {
        this.sink = sink;
    }

    TestEntitySorter() {
        this.entities = new ArrayList<EntityContainer>();

    }

    @Override
    public void process(EntityContainer entityContainer) {
        entities.add(entityContainer);
    }

    public void complete() {
        EntityContainerComparator c = new EntityContainerComparator(new EntityByTypeThenIdComparator());

        Collections.sort(entities, c);


        for (EntityContainer ec  : entities) {
            sink.process(ec);
        }

        sink.complete();
    }

    @Override
    public void release() {

    }

    @Override
    public void initialize(Map<String, Object> metaData) {

    }
}
