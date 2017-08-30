package org.openstreetmap.osmosis.hbase.analysis;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorExportToWkb;
import com.esri.core.geometry.examples.ShapefileGeometryCursor;
import com.google.inject.Guice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.hbase.HBaseWriter;
import org.openstreetmap.osmosis.hbase.MapReduceUnitSetup;
import org.openstreetmap.osmosis.hbase.MockHTableModule;
import org.openstreetmap.osmosis.hbase.common.EntityDataAccess;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.common.WaySerDe;
import org.openstreetmap.osmosis.hbase.mr.analysis.RoadlessMap;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 28-Oct-16.
 */
public class RoadlessMapTest extends MapReduceUnitSetup {


    @Before
    public void init() throws Exception {
        injector = Guice.createInjector(new MockHTableModule());
    }


    @Test
    public void testMRShpInput() throws IOException {

        ImmutableBytesWritable key = new ImmutableBytesWritable();
        MapReduceDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation> mapReduceDriver = getMapReduceDriver();

        FileInputStream fileInputStream = new FileInputStream(new File("src/test/resources/shp/canary.shp"));

        ShapefileGeometryCursor shapeFileReader = new ShapefileGeometryCursor(fileInputStream);

//        OperatorImportFromWkb local = OperatorImportFromWkb.local();
        OperatorExportToWkb local = OperatorExportToWkb.local();
        while (shapeFileReader.hasNext()) {

            Geometry next = shapeFileReader.next();

            key.set(Bytes.toBytes(shapeFileReader.getGeometryID()));
            ByteBuffer wkbByteBuffer = local.execute(0, next, null);

            byte[] array = wkbByteBuffer.array();
            KeyValue kv = new KeyValue(key.get(), EntityDataAccess.data, WaySerDe.geom, array);
            List<Cell> keyValues = new ArrayList<Cell>();
            keyValues.add(kv);
            Result result = Result.create(keyValues);
            mapReduceDriver.withInput(key, result);

        }

//        while (shapeFileReader.hasNext()) {
//        }
//        for (Result result : wayScanner) {
//            row.set(result.getRow());
//        }

        mapReduceDriver.run();
    }

    protected MapReduceDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation> getMapReduceDriver() {
        MapReduceDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation> mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        setupSerialization(mapReduceDriver);

        mapReduceDriver.setMapper(new RoadlessMap.WayTableMapper());
        mapReduceDriver.setReducer(new RoadlessMap.BufferReducer());
        return mapReduceDriver;
    }

    @Test
    public void testMR() throws IOException {

        /*
         * fixme move table loading to superclass??
         */
        File snapshotFile = dataUtils.createDataFile("v0_6/db-snapshot.osm");
        // Generate input files.
        //two for one
        HBaseWriter changeWriter = injector.getInstance(HBaseWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(snapshotFile, true, CompressionMethod.None);
        xmlReader.setSink(changeWriter);
        xmlReader.run();


        MapReduceDriver<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation> mapReduceDriver = getMapReduceDriver();

        TableFactory tableFactory = getTableFactory();

        Table ways = tableFactory.getTable("ways");
        Scan scan = new Scan().addFamily(EntityDataAccess.data).addFamily(EntityDataAccess.tags);
        ResultScanner wayScanner = ways.getScanner(scan);
        ImmutableBytesWritable row = new ImmutableBytesWritable();
        for (Result result : wayScanner) {
            row.set(result.getRow());
            mapReduceDriver.withInput(row, result);
        }

        mapReduceDriver.run();
    }
}
