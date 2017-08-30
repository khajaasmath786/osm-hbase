package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;

import java.util.ArrayList;
import java.util.List;

/**
 * Attempt at using Structs.
 *
 * Created by willtemperley@gmail.com on 18-Jul-16.
 */
public class RelationSerDe extends EntitySerDe<Relation> {

    private final Struct struct;
    private final PositionedByteRange positionedByteRange = new SimplePositionedMutableByteRange();
    private static final EntityType[] entityTypes = EntityType.values();

//    values = EntityType.g.values()

    private static final byte[] relationColPrefix = "r".getBytes();

    public RelationSerDe() {

        /*
        Seems that strings have to be in the right-most position as they are variable length
        Can't have two strings
         */
        StructBuilder structBuilder = new StructBuilder();
        structBuilder.add(new RawLong());//member id
        structBuilder.add(new RawInteger());//member type
        structBuilder.add(RawString.ASCENDING);//member role
        struct = structBuilder.toStruct();
    }

    @Override
    public int getEntityType() {
        return EntityType.Relation.ordinal();
    }

    //    class RelationMember extends PBType
    @Override
    public void encode(byte[] rowKey, Relation entity, List<Cell> keyValues) {

        List<RelationMember> members = entity.getMembers();

        for (short i = 0; i < members.size(); i++) {
            byte[] bytes = encodeRelationMember(members.get(i));
            byte[] colName = getRelationMemberColumn(i);
            Cell cell = getDataCellGenerator().getKeyValue(rowKey, colName, bytes);
            keyValues.add(cell);
        }

    }

    private byte[] getRelationMemberColumn(short i) {
        return Bytes.add(relationColPrefix, Bytes.toBytes(i));
    }

    private byte[] encodeRelationMember(RelationMember rm) {

        //Store an int instead of a string for the enum
        int memberTypeEnumOrdinal = rm.getMemberType().ordinal();

        positionedByteRange.set(new byte[8 + 4 + rm.getMemberRole().getBytes().length]);
        Object[] val = {rm.getMemberId(), memberTypeEnumOrdinal, rm.getMemberRole()};
        struct.encode(positionedByteRange, val);
        return positionedByteRange.getBytes();
    }


    @Override
    public Relation constructEntity(Result result, CommonEntityData commonEntityData) {

        List<RelationMember> relationMembers = new ArrayList<RelationMember>();
        short i = 0;
        while (true) {
            byte[] value = result.getValue(data, getRelationMemberColumn(i));
            if (value == null) {
                break;
            }
            positionedByteRange.set(value);
            Object[] objects = struct.decode(positionedByteRange);

            Long memberId = (Long) objects[0];
            Integer memberType = (Integer) objects[1];
            String memberRole = (String) objects[2];

            //get type from ordinal
            EntityType entityType = entityTypes[memberType];

            RelationMember roleMember = new RelationMember(
                    memberId,
                    entityType,
                    memberRole
            );
            relationMembers.add(roleMember);
            i++;
        }

        return new Relation(commonEntityData, relationMembers);
    }
}
