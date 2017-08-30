package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.*;

/**
 * The basic machinery to ser/deserialize an osm entity.
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public abstract class EntitySerDe<T extends Entity> extends EntityDataAccess {

    protected CellGenerator dataKvGen;

    /**
     * Generate cells for the main column family
     *
     * @param rowKey the entity row key
     * @param entity the OSM entity
     * @return all columns in the data col fam
     */
    public List<Cell> dataKeyValues(byte[] rowKey, T entity) {

        dataKvGen = new CellGenerator(data);

        List<Cell> keyValues = new ArrayList<Cell>();

        keyValues.add( dataKvGen.getKeyValue(rowKey, version, entity.getVersion()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, timestamp, entity.getTimestamp().getTime()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, changeset, entity.getChangesetId()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, uid, entity.getUser().getId()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, uname, entity.getUser().getName()) );
        keyValues.add( dataKvGen.getKeyValue(rowKey, entitytype, getEntityType()) );

        encode(rowKey, entity, keyValues);

        return keyValues;
    }

    public abstract int getEntityType();


//    public List<Cell> tagKeyValues(byte[] rowKey, Entity entity) {
//
//        List<Cell> keyValues = new ArrayList<Cell>();
//        CellGenerator tagKvGen = new CellGenerator(tags);
//        for (Tag tag : entity.getTags()) {
//            keyValues.add( tagKvGen.getKeyValue(rowKey, tag.getKey().getBytes(), tag.getValue()) );
//        }
//        return keyValues;
//    }

    public List<Cell> tagKeyValues(byte[] rowKey, Collection<Tag> entityTags) {

        List<Cell> keyValues = new ArrayList<Cell>();
        CellGenerator tagKvGen = new CellGenerator(tags);
        for (Tag tag : entityTags) {
            keyValues.add( tagKvGen.getKeyValue(rowKey, tag.getKey().getBytes(), tag.getValue()) );
        }
        return keyValues;
    }

    Put serialize(T entity) {

        byte[] rowKey = getRowKey(entity);
        Put put = new Put(rowKey);

        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();

        familyCellMap.put(data, dataKeyValues(rowKey, entity));
        familyCellMap.put(tags, tagKeyValues(rowKey, entity.getTags()));

        return put;
    }

    protected abstract void encode(byte[] rowKey, T entity, List<Cell> keyValues);

    public T deSerialize(Result result) {
        return constructEntity(result, getCommonEntityData(result));
    }

    protected abstract T constructEntity(Result result, CommonEntityData commonEntityData);

    private CommonEntityData getCommonEntityData(Result result) {

        long id = getId(result.getRow());
        int v = getInt(version, result);
        long ts = getLong(timestamp, result);
        long cs = getLong(changeset, result);

        Date date = new Date(ts);

        int anInt = getInt(uid, result);
        String string = getString(uname, result);

        OsmUser osmUser = OsmUser.NONE;
        if (anInt != -1) {
            osmUser = new OsmUser(anInt, string);
        }

        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(tags);

        Set<Tag> tags = new HashSet<Tag>(familyMap.size());
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String key = Bytes.toString(entry.getKey());
            String value = Bytes.toString(entry.getValue());
            tags.add(new Tag(key, value));
        }

        return new CommonEntityData(id, v, date, osmUser, cs, tags);

    }

    public CellGenerator getDataCellGenerator() {
        return dataKvGen;
    }
}
