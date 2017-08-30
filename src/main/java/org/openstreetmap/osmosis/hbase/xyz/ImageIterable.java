package org.openstreetmap.osmosis.hbase.xyz;

import xyz.wgs84.TileKey;

/**
 * Created by willtemperley@gmail.com on 15-Mar-17.
 */
public abstract class ImageIterable {

    private final TileKey tileKey;
    private final double pixelSizeY;
    private final double pixelSizeX;
    private final int h;
    private final int w;
    private boolean touched;
    private int col;
    private double[] coordinate = new double[3];

    /*
    Could be generified with a databuffer (float etc)
    */
    public ImageIterable(TileKey tileKey) {
        this.tileKey = tileKey;
        this.col = 0;
        this.w = tileKey.getWidth();
        this.h = tileKey.getWidth();
        this.pixelSizeX = tileKey.getPixelSizeX();
        this.pixelSizeY = tileKey.getPixelSizeY();

        //todo: do we need the pixel centre or top left? going with centre
        coordinate[0] = tileKey.getOriginX() + (pixelSizeX / 2);
        coordinate[1] = tileKey.getOriginY() - (pixelSizeY / 2);

        touched = false;
    }

    public abstract boolean hasNext();

    void increment() {
        if (!touched) {
            touched = true;
            return;
        }
        col++;
        coordinate[0] += pixelSizeX;
        if (col >= w) {
            col = 0;
            coordinate[0] = tileKey.getOriginX() + (pixelSizeX / 2);
            coordinate[1] -= pixelSizeY;
        }
    }

    abstract int get();

    public int getValue() {
        increment();
        return get();
    }

    /**
     * The coordinate is set when getValue is called
     *
     * @return
     */
    public double[] getCoordinate() {
//        coordinate[0] = col * pixelSizeX
        return coordinate;
    }
}
