package org.openstreetmap.osmosis.hbase.xyz.cmd;


import org.geotools.resources.Arguments;
import org.openstreetmap.osmosis.hbase.xyz.ImageTiler;
import org.openstreetmap.osmosis.hbase.xyz.ImageSeqFileWriter;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TileToSequenceFile extends ImageTiler {

    public TileToSequenceFile() {
        super(1024);
    }

    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);

        TileToSequenceFile tiler = new TileToSequenceFile();

        String inPath  = processedArgs.getRequiredString("-f");
        String outPath = processedArgs.getRequiredString("-o");

        ImageSeqFileWriter writer = new ImageSeqFileWriter(outPath);
        tiler.doTiling(inPath, writer);
    }

}
