package org.openstreetmap.osmosis.hbase.xyz;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Point;
import org.junit.Assert;
import org.junit.Test;
import xyz.wgs84.TileKey;

import java.nio.ByteBuffer;

/**
 * Created by willtemperley@gmail.com on 15-Mar-17.
 */
public class ImageIterableTest {


    @Test
    public void hasNext()  {

        TileKey serKey = new TileKey();

        int w = 100;
        int h = 200;
        serKey.setDimensions(w, h);
        serKey.setPixelSize(0.1, 0.2);
        serKey.setOrigin(11.1, 22.2);
        serKey.setProj(4326);

        Envelope2D envelope2D = serKey.getEnvelope2D();
        double scaleFactor = 1.00000001; //a little tolerance for floating point errors
        envelope2D = envelope2D.getInflated(scaleFactor, scaleFactor);

        /*
        A dummy array of ints
         */
        int[] ints = new int[w * h];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = i;
        }

        ImageIterable imageIterable = new ImageIterableInt(serKey, ints);
        testImageIterable(envelope2D, imageIterable);

        //same data in byte form
        byte[] bytes = new byte[100 * 200 * 4];
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        for (int i = 0; i < ints.length; i++) {
            wrapped.putInt(i);
        }

        ImageIterable imageIterableByte = new ImageIterableByte(serKey, wrapped);
        testImageIterable(envelope2D, imageIterableByte);
    }

    private void testImageIterable(Envelope2D envelope2D, ImageIterable imageIterable) {
        int i = 0;
        while (imageIterable.hasNext()) {
            int value = imageIterable.getValue();
            double[] coordinate = imageIterable.getCoordinate();
            imageIterable.increment();//fixme horror

            //Values should come in the same order they went in!
            Assert.assertTrue(value == i);

            Point point = new Point(coordinate[0], coordinate[1]);
//            System.out.println("point = " + point);
//            System.out.println("value = " + value);
            boolean contains = envelope2D.contains(point);
            Assert.assertTrue(contains);

            i++;
        }
    }

}