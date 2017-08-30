package org.openstreetmap.osmosis.hbase.mr.text;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.hbase.common.Entity;
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess;
import org.openstreetmap.osmosis.hbase.common.EntitySerDe;
import org.openstreetmap.osmosis.hbase.mr.BlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;

import java.io.IOException;
import java.util.List;

/**
 *
 * Created by willtemperley@gmail.com on 28-Jul-16.
 */
public abstract class TextEntityMapper<T extends Entity> extends Mapper<Text, ArrayPrimitiveWritable, ImmutableBytesWritable, Cell> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.getConfiguration().get(EntityType.class.getName());
    }

    public TextEntityMapper(EntityType entityType, EntitySerDe<T> entitySerDe) {
        this.entityType = entityType;
        this.entitySerDe = entitySerDe;
    }

    final EntityType entityType;
    final EntitySerDe<T> entitySerDe;

    ImmutableBytesWritable k = new ImmutableBytesWritable();

    @Override
    protected void map(Text key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {
        byte[] bytes = (byte[]) value.get();

        List<EntityContainer> ecs = readBlob(bytes, key.toString());
        for (EntityContainer ec : ecs) {

            org.openstreetmap.osmosis.core.domain.v0_6.Entity e = ec.getEntity();

            if (e.getType().equals(entityType)) {

                byte[] rowKey = EntityDataAccess.getRowKey(e.getId());
                k.set(rowKey);

                for (Cell cell : entitySerDe.tagKeyValues(rowKey, e.getTags())) {
                    context.write(k, cell);
                }

                for (Cell cell : entitySerDe.dataKeyValues(rowKey, getEntity(e))) {
                    context.write(k, cell);
                }
            }
        }
    }

    /**
     * Creates a wrapped entity from the OSM entity via a cast
     *
     * @param entity  osmosis entity
     * @return the cast and wrapped entity
     */
    abstract T getEntity(org.openstreetmap.osmosis.core.domain.v0_6.Entity entity);

    List<EntityContainer> readBlob(byte[] bytes, String osmDataType) {

        BlobDecoderListener decoderListener = new BlobDecoderListener();
        PbfBlobDecoder blobDecoder = new PbfBlobDecoder(osmDataType, bytes, decoderListener);
        blobDecoder.run();
        return decoderListener.getEntityContainers();
    }
}
