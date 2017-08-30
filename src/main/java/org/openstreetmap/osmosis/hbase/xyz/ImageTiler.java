package org.openstreetmap.osmosis.hbase.xyz;

import org.openstreetmap.osmosis.hbase.xyz.geotiff.GeoTiffReader;
import xyz.wgs84.TileKey;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 14-Mar-17.
 */
public class ImageTiler {

    private final int targetTileSize;

    public ImageTiler(int tileSize) {

        this.targetTileSize = tileSize;
//        this.targetTileSize = 100;
    }

    public void doTiling(String outputFilePath, TileWriter writer) throws IOException {

        GeoTiffReader geoTiffReader = new GeoTiffReader();
        GeoTiffReader.ReferencedImage referencedImage = geoTiffReader.readGeotiffFromFile(new File(outputFilePath));

        GeoTiffReader.ImageMetadata metadata = referencedImage.getMetadata();
        int w = metadata.getWidth();
        int h = metadata.getHeight();

        com.esri.core.geometry.Envelope2D imgEnv = metadata.getEnvelope2D();

        double geographicPixHeight = (imgEnv.ymax - imgEnv.ymin) / (double) h;

        RenderedImage renderedImage = referencedImage.getRenderedImage();

        double nTilesX = Math.ceil(((double) w) / targetTileSize);
        double nTilesY = Math.ceil(((double) h) / targetTileSize);

        int whereAreWe = 0;

        ImageTileWritable imageTileWritable = new ImageTileWritable();

        Raster data = renderedImage.getData();
        //fixme hack for non GEE images
        for (int i = 0; i < nTilesX; i++) {
            int sizeX = targetTileSize;
            if ((i + 1) * targetTileSize > w) {
                sizeX = w - (i * targetTileSize);
            }
            int tileOrigX = i * sizeX;
            double geoOffsetLeft = (i * targetTileSize) * geographicPixHeight;

            for (int j = 0; j < nTilesY; j++) {
                int sizeY = targetTileSize;
//                int whereWeAre = (j + 1) * targetTileSize;
                if ((j + 1) * targetTileSize > h) {
                    sizeY = h - (j * targetTileSize);
                }
                int tileOrigY = j * sizeY;
                int[] IMAGE = new int[sizeX * sizeY];
                data.getPixels(tileOrigX, tileOrigY, sizeX, sizeY, IMAGE);
                TileKey tileKey = new TileKey();
                double geoOffsetTop = (j * targetTileSize) * geographicPixHeight;
                tileKey.setOrigin(imgEnv.xmin + geoOffsetLeft, imgEnv.ymax - geoOffsetTop);
                tileKey.setDimensions(sizeX, sizeY);
                tileKey.setProj(4326);
                tileKey.setPixelSize(geographicPixHeight, geographicPixHeight);
                writer.append(tileKey, IMAGE);
            }
        }

        writer.close();
    }
}
