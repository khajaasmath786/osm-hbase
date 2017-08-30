package org.openstreetmap.osmosis.hbase.xyz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import xyz.wgs84.TileKey;

import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class ImageSeqFileWriter implements TileWriter {

    protected ImageTileWritable v = new ImageTileWritable();
    protected TileKeyWritable k = new TileKeyWritable();

    private final SequenceFile.Writer writer;

    public ImageSeqFileWriter(String outputDirectory) throws IOException {
        Configuration conf = new Configuration();
        SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(outputDirectory));

            this.writer = SequenceFile.createWriter(conf, fileOption,
                    SequenceFile.Writer.keyClass(TileKeyWritable.class),
                    SequenceFile.Writer.valueClass(ImageTileWritable.class)
            );
    }

    int i = 0;
    public void append(TileKey key, int[] image) throws IOException {
        System.out.println("i = " + i);
        k.setTile(key, ++i);
        v.setImage(image);
        writer.append(k, v);
    }

    public void close() throws IOException {
        writer.close();
    };
}
