package org.openstreetmap.osmosis.hbase.mr.writable;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
  * GenericWritables allow a polymorphic type to be passed to the reducer, with minimised overhead.
  * This simply allows all entities to be written as one type and the framework knows how to deserialize based on it's class.
  *
  * Created by willtemperley@gmail.com on 11-Mar-16.
  */
public class OsmEntityWritable extends GenericWritable {

    private static Class[] classes = new Class[]{
        WayNodeWritable.class,
        NodeWritable.class
    };

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return classes;
    }


}
