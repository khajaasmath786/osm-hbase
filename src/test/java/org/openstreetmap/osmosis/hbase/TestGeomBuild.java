package org.openstreetmap.osmosis.hbase;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.extract.EntityListDumper;
import org.openstreetmap.osmosis.hbase.extract.FeatureDataExtractor;
import org.openstreetmap.osmosis.testutil.AbstractDataTest;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
import org.openstreetmap.osmosis.xml.v0_6.XmlWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 *
 * Created by willtemperley@gmail.com on 30-Aug-16.
 */
public class TestGeomBuild extends AbstractDataTest {

    protected Injector injector;

    @Before
    public void init() throws Exception {
        injector = Guice.createInjector(new MockHTableModule());
    }

    @Test
    public void go () throws IOException {

        File dataFile = dataUtils.createDataFile("v0_6/db-snapshot2rel.osm");

        HBaseWriter hBaseWriter = injector.getInstance(HBaseWriter.class);

        //read
        XmlReader xmlReader = new XmlReader(dataFile, true, CompressionMethod.None);
        xmlReader.setSink(hBaseWriter);

        xmlReader.run();

        FeatureDataExtractor featureDataExtractor = new FeatureDataExtractor(injector.getInstance(TableFactory.class));

        List<EntityContainer> relation = featureDataExtractor.getRelation(1L);

        for (EntityContainer entityContainer : relation) {
            System.out.println("entityContainer = " + entityContainer);
        }

        EntityListDumper entityListDumper = new EntityListDumper(relation);

        writeHBaseDataToXml(entityListDumper, new File("E:/tmp/x.osm.xml"));

//        FeatureCollection featureCollection = relationBuilder.getRelationAsJson(2715959);
//        String json= new ObjectMapper().writeValueAsString(featureCollection);
//        System.out.println(json);
//
//        relationBuilder.getWay(414419509);
//        RelationBuilder relationBuilder = new RelationBuilder()
    }

    private static void writeHBaseDataToXml(EntityListDumper dumpDataset, File actualResultFile) {

        XmlWriter writer = new XmlWriter(actualResultFile, CompressionMethod.None);

        //dataset sunk into sorter
        dumpDataset.setSink(writer);

        //sorter sunk into writer
        dumpDataset.process(null);
    }

}
