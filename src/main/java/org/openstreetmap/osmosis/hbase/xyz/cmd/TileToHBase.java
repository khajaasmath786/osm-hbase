package org.openstreetmap.osmosis.hbase.xyz.cmd;


import org.apache.hadoop.hbase.client.Table;
import org.geotools.resources.Arguments;
import org.openstreetmap.osmosis.hbase.HTableFactory;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.xyz.HBaseTileWriter;
import org.openstreetmap.osmosis.hbase.xyz.ImageTiler;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TileToHBase extends ImageTiler {

    public TileToHBase(int tileSize) {
        super(tileSize);
    }

    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);

        String inPath  = processedArgs.getRequiredString("-f");
        String outTable = processedArgs.getRequiredString("-o");
        int tileSize = processedArgs.getRequiredInteger("-t");

        TileToHBase tiler = new TileToHBase(tileSize);

        TableFactory tableFactory = new HTableFactory();
        Table table = tableFactory.getTable(outTable);
        HBaseTileWriter writer = new HBaseTileWriter(table);
        tiler.doTiling(inPath, writer);
    }

}
