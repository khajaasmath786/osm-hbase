package org.openstreetmap.osmosis.hbase.extract;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.hbase.TableModule;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Utility to extract a relation from db
 *
 * Created by willtemperley@gmail.com on 23-Aug-16.
 */
public class ExtractWay {


    /**
     * http://www.openstreetmap.org/relation/1443024#map=9/-29.3151/29.5999
     *
     */
    public static void main(String[] args) throws IOException {

        Injector objectGraph = Guice.createInjector(new TableModule());
        long wayId = Long.valueOf(args[0]);
        String pathname = args[1];

        TableFactory tableFactory = objectGraph.getInstance(TableFactory.class);

        FeatureDataExtractor featureDataExtractor = new FeatureDataExtractor(tableFactory);

        List<EntityContainer> relation = featureDataExtractor.getWay(wayId);

        EntityListDumper entityListDumper = new EntityListDumper(relation);

        writeHBaseDataToXml(entityListDumper, new File(pathname));
    }


    private static void writeHBaseDataToXml(EntityListDumper dumpDataset, File actualResultFile) {

        XmlWriter writer = new XmlWriter(actualResultFile, CompressionMethod.None);

        //dataset sunk into sorter
        dumpDataset.setSink(writer);

        //sorter sunk into writer
        dumpDataset.process(null);
    }
}
