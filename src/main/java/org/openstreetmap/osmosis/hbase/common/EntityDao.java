package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.*;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DAOs and serdes are separated to allow reuse of SerDes in e.g. iterators
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public abstract class EntityDao<T extends Entity> {

    private final EntitySerDe<T> serde;

    EntityDao(Table table) {
        this.table = table;
        this.serde = getSerDe();
    }

    final Table table;


    public void process(T entity, ChangeAction action) {

        if (action.equals(ChangeAction.Create)) {
            put(entity);
        } else if (action.equals(ChangeAction.Modify)) {
            put(entity);
        } else if (action.equals(ChangeAction.Delete)) {
            delete(entity);
        } else {
            throw new RuntimeException("Unknown action: " + action);
        }

    }

    private void delete(T entity) {

        Delete delete = new Delete(EntityDataAccess.getRowKey(entity));
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean exists(long entityId) {
        Get get = new Get(EntityDataAccess.getRowKey(entityId));
        try {
            return table.exists(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> get(long[] entityIds) {

        List<Get> gets = new ArrayList<Get>(entityIds.length);
        List<T> entities = new ArrayList<T>(entityIds.length);

        for (int i = 0; i < entityIds.length; i++) {

            byte[] rowKey = EntityDataAccess.getRowKey(entityIds[i]);
            Get get = new Get(rowKey); //all cols from all families
            gets.add(i, get);
        }

        try {
            Result[] results = table.get(gets);

            for (int i = 0; i < results.length; i++) {
                entities.add(i, serde.deSerialize(results[i]));
            }
            return entities;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public T get(long entityId) {

        byte[] rowKey = EntityDataAccess.getRowKey(entityId);

        Get get = new Get(rowKey);
        get.addFamily(EntityDataAccess.data);
        get.addFamily(EntityDataAccess.tags);
        try {

            Result result = table.get(get);
            return serde.deSerialize(result);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    public void put(T entity) {

        //No point in having a whole load of machinery to process this
        if (entity.getType().equals(EntityType.Bound)) {
            return;
        }

        //A put with the timestamp of the entity

        Put put = serde.serialize(entity);

        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public abstract EntitySerDe<T> getSerDe();

}
