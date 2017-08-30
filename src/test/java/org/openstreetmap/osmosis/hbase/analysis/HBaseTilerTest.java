package org.openstreetmap.osmosis.hbase.analysis;

import com.esri.core.geometry.Envelope2D;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.vividsolutions.jts.geom.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.hbase.MapReduceUnitSetup;
import org.openstreetmap.osmosis.hbase.MockHTableModule;
import org.openstreetmap.osmosis.hbase.mr.analysis.ImageRegions;
import org.openstreetmap.osmosis.hbase.mr.analysis.TileMapper;
import org.openstreetmap.osmosis.hbase.mr.analysis.TileStatsSerDe;
import org.openstreetmap.osmosis.hbase.mr.analysis.TileTableReducer;
import org.openstreetmap.osmosis.hbase.utility.MockHTableFactory;
import org.openstreetmap.osmosis.hbase.xyz.*;
import org.openstreetmap.osmosis.hbase.xyz.cmd.TileToHBase;
import org.roadlessforest.imageregions.DebugMercatorTile;
import org.roadlessforest.imageregions.DebugPoint;
import org.roadlessforest.imageregions.DebugTile;
import org.roadlessforest.imageregions.EMF;
import xyz.mercator.MercatorTile;
import xyz.mercator.MercatorTileCalculator;
import xyz.wgs84.TileKey;

import javax.persistence.EntityManager;
import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by willtemperley@gmail.com on 14-Mar-17.
 */
public class HBaseTilerTest extends MapReduceUnitSetup {

    private Injector injector;
    private final String tableName = "tiled";
    MockHTableFactory hTableFact;
    static GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

    /**
     * Loads up the tiles (it's a test in itself)
     *
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        injector = Guice.createInjector(new MockHTableModule());

        hTableFact = injector.getInstance(MockHTableFactory.class);
        TileToHBase tiler = new TileToHBase(10);

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resourceStream = loader.getResource("tif/littletiff.tif");

        Table tiled = hTableFact.getTable(tableName);

        HBaseTileWriter writer = new HBaseTileWriter(tiled);
        tiler.doTiling(resourceStream.getPath(), writer);
    }

    public static Map<MercatorTile, Integer> getImageStatistics(int whichInt) throws IOException {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resourceStream = loader.getResource("littletiffcounts.tsv");

        BufferedReader bufferedReader = new BufferedReader(new FileReader(resourceStream.getFile()));

        Map<MercatorTile, Integer> tileCountMap = new HashMap<>();
        String line;
        while ((line = bufferedReader.readLine()) != null) {

            String[] split = line.split("\t");
            Integer x = Integer.valueOf(split[0]);
            Integer y = Integer.valueOf(split[1]);
            Integer c = -1;
            if (whichInt == 71) {
                c = Integer.valueOf(split[2]);
            } else if (whichInt == 72) {
                c = Integer.valueOf(split[3]);
            }

            MercatorTile mercatorTile = new MercatorTile();
            mercatorTile.setX(x);
            mercatorTile.setY(y);
            mercatorTile.setZ(14);

            tileCountMap.put(mercatorTile, c);
        }

        return tileCountMap;
    }

    public static void TODO(String[] args) {
        Envelope2D envelope2D = new Envelope2D();
        //little tiff
        envelope2D.setCoords(0.149838, 10.14997, 0.27003, 10.22004);

        MercatorTileCalculator mercatorTileCalculator = new MercatorTileCalculator();
        List<MercatorTile> mercatorTiles = mercatorTileCalculator.tilesForEnvelope(envelope2D, 14);

        EntityManager em = EMF.getEM();
        em.getTransaction().begin();
        for (MercatorTile mercatorTile : mercatorTiles) {
            Envelope2D tileEnvelope = mercatorTileCalculator.getTileEnvelope(mercatorTile);

            DebugMercatorTile debugMercatorTile = new DebugMercatorTile();
            debugMercatorTile.setTileX(mercatorTile.getX());
            debugMercatorTile.setTileY(mercatorTile.getY());
            debugMercatorTile.setGeom(jtsGeomFromEsriEnv(tileEnvelope));
//            tileEnvelope.
            em.persist(debugMercatorTile);
        }
        em.getTransaction().commit();

    }

    public static Geometry jtsGeomFromEsriEnv(Envelope2D envelope2D) {

        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(envelope2D.xmin, envelope2D.ymin);
        coordinates[1] = new Coordinate(envelope2D.xmin, envelope2D.ymax);
        coordinates[2] = new Coordinate(envelope2D.xmax, envelope2D.ymax);
        coordinates[3] = new Coordinate(envelope2D.xmax, envelope2D.ymin);
        coordinates[4] = coordinates[0];
        return geometryFactory.createPolygon(coordinates);
    }

    private void writeDebugTile(TileKey key, byte[] val) {
        EntityManager em = EMF.getEM();
        em.getTransaction().begin();

        DebugTile debugTile = new DebugTile();
        Envelope2D envelope2D = key.getEnvelope2D();

        Geometry polygon = jtsGeomFromEsriEnv(envelope2D);
        debugTile.setGeom(polygon);
        debugTile.setId(key.toString());
        em.persist(debugTile);

        ImageIterable imageIterable = new ImageIterableByte(key, ByteBuffer.wrap(val));
        while (imageIterable.hasNext()) {
            int value = imageIterable.getValue();
            if (value != 101) {

                System.out.println("value = " + value);
                double[] coordinate = imageIterable.getCoordinate();
                DebugPoint debugPoint = new DebugPoint();
                Point point = geometryFactory.createPoint(new Coordinate(coordinate[0], coordinate[1]));
                debugPoint.setPoint(point);
                debugPoint.setValue(value);
                em.persist(debugPoint);
            }
        }

        em.getTransaction().commit();
        System.out.println("commit");
    }

    @Test
    public void testTableMapReduce() throws IOException {

//        TableFactory hTableFact = getTableFactory();

        MapReduceDriver<ImmutableBytesWritable, Result, WebTileWritable, IntWritable, WebTileWritable, Mutation> mapReduceDriver
                = MapReduceDriver.newMapReduceDriver();
        Table mockTable = hTableFact.getTable(tableName);
        System.out.println("mockTable = " + mockTable);

        mapReduceDriver.setMapper(new TileMapper());
        mapReduceDriver.setReducer(new TileTableReducer());

        //Set up config with some settings that would normally be set in HFileOutputFormat2.configureIncrementalLoad();
        setupSerialization(mapReduceDriver);
        mapReduceDriver.getConfiguration().set(ImageRegions.N_HISTOGRAM_BINS, "110");

        Scan scan = new Scan();
        scan.addColumn(TileKeyWritable.dataCF, TileKeyWritable.imageC);

        TileKeyWritable writable = new TileKeyWritable();

        ResultScanner scanner = mockTable.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
            writable.set(result.getRow());
            mapReduceDriver.withInput(new Pair<ImmutableBytesWritable, Result>(writable, result));
        }


        //Expected
        Map<MercatorTile, Integer> imageStatistics = getImageStatistics(71);
        //Actual
        List<Pair<WebTileWritable, Mutation>> run = mapReduceDriver.run();

        Table mockStatsTable = hTableFact.getTable("stats");

        MercatorTile mercatorTile = new MercatorTile();
        for (Pair<WebTileWritable, Mutation> webTileWritableTextPair : run) {
            WebTileWritable tileWritable = webTileWritableTextPair.getFirst();
            tileWritable.getTile(mercatorTile);
            Mutation second = webTileWritableTextPair.getSecond();

            mockStatsTable.put((Put) second);
        }

        processResults(mockStatsTable);
    }

    private void processResults(Table mockStatsTable) throws IOException {
        //looking at one value at a time
        int toTest = 71;

        //Expected
        Map<MercatorTile, Integer> imageStatistics = getImageStatistics(toTest);

        WebTileWritable webTileWritable = new WebTileWritable();
        MercatorTile tile = new MercatorTile();

        TileStatsSerDe tileStatsSerDe = new TileStatsSerDe();

        Scan scan = new Scan();
        scan.addFamily(TileStatsSerDe.cfD);
        ResultScanner scanner = mockStatsTable.getScanner(scan);

        Result result;
        while ((result = scanner.next()) != null) {
            byte[] row = result.getRow();
            webTileWritable.set(row);
            webTileWritable.getTile(tile);
            System.out.println("tile = " + tile);
            Map<Integer, Integer> integerIntegerMap = tileStatsSerDe.deSerialize(result);
            for (Map.Entry<Integer, Integer> integerIntegerEntry : integerIntegerMap.entrySet()) {
                Integer key = integerIntegerEntry.getKey();
                if (key == toTest) {
                    Integer expected = imageStatistics.get(tile);
                    Integer actual = integerIntegerEntry.getValue();
                    Assert.assertEquals(expected, actual);
                }
            }
        }
    }

    @Test
    public void testMapReduce() throws IOException {

//        TableFactory hTableFact = getTableFactory();

        MapReduceDriver<ImmutableBytesWritable, Result, WebTileWritable, IntWritable, WebTileWritable, Text> mapReduceDriver
                = MapReduceDriver.newMapReduceDriver();
        Table mockTable = hTableFact.getTable(tableName);
        System.out.println("mockTable = " + mockTable);

        mapReduceDriver.setMapper(new TileMapper());
        mapReduceDriver.setReducer(new ImageRegions.TileReducer());

        //Set up config with some settings that would normally be set in HFileOutputFormat2.configureIncrementalLoad();
        setupSerialization(mapReduceDriver);

        Scan scan = new Scan();
        scan.addColumn(TileKeyWritable.dataCF, TileKeyWritable.imageC);

        TileKeyWritable writable = new TileKeyWritable();

        ResultScanner scanner = mockTable.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
            writable.set(result.getRow());
            mapReduceDriver.withInput(new Pair<ImmutableBytesWritable, Result>(writable, result));
        }


        //Expected
        Map<MercatorTile, Integer> imageStatistics = getImageStatistics(71);
        //Actual
        List<Pair<WebTileWritable, Text>> run = mapReduceDriver.run();

        MercatorTile mercatorTile = new MercatorTile();
        for (Pair<WebTileWritable, Text> webTileWritableTextPair : run) {
            WebTileWritable tileWritable = webTileWritableTextPair.getFirst();
            tileWritable.getTile(mercatorTile);
            Text second = webTileWritableTextPair.getSecond();
            String text = Bytes.toString(second.getBytes());
            String[] split = text.split(":");

            Integer val   = Integer.valueOf(split[0]);
            Integer actual = Integer.valueOf(split[1]);

            if (val == 71) {
                Integer expected = imageStatistics.get(mercatorTile);
                System.out.println("mercatorTile = " + mercatorTile);
                Assert.assertEquals(expected, actual);
            }
        }

//        while (streamSplitter.hasNext()) {
//            PbfRawBlob blob = streamSplitter.next();
//            arrayPrimitiveWritable.set(blob.getData());
//            String type = blob.getType();
//            text.set(type);
//            mapReduceDriver.withInput(text, arrayPrimitiveWritable);
//        }

        //Retrieve MR results
//        List<Pair<ImmutableBytesWritable, Put>> results = mapReduceDriver.run();
//        for (Pair<ImmutableBytesWritable, Put> cellPair : results) {
//            mockTable.put(cellPair.getSecond());
//        }
    }


}
