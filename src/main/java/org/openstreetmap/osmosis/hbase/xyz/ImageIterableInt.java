package org.openstreetmap.osmosis.hbase.xyz;

import xyz.wgs84.TileKey;

/**
 *
 * Created by willtemperley@gmail.com on 15-Mar-17.
 */
public class ImageIterableInt extends ImageIterable {

    private final int[] ints;
    private int i;

    public ImageIterableInt(TileKey tileKey, int[] ints) {
        super(tileKey);
        this.ints = ints;
    }

    @Override
    public boolean hasNext() {
        return i < ints.length;
    }

    @Override
    int get() {
        i++;
        return ints[i-1];
    }
}
