package org.openstreetmap.osmosis.hbase;

import com.google.common.primitives.Longs;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by willtemperley@gmail.com on 29-Aug-16.
 */
public class KeyDistributionTest {

    /**
     * Illustrates how reversing the row key creates an almost perfect distribution over the range 00 to FF for the first bytes of a monotonically increasing
     * integer
     */
    @Test
    public void testKeyDistribution() {
        long[] keys = new long[1000000];

        int i = 1000000 - 1;
        while (i > 0) {
            double x = Math.random();

            int y = (int) (x * 10);

            if (y != 3) {
                keys[i] = i;
                i--;
            }
        }
        testKeyDistribution(keys);
    }

    public void testKeyDistribution(long[] keys) {
        long[] freq = new long[256];

        for (int i = 0; i < keys.length; i++) {

            byte[] bytes = Bytes.toBytes(i);
            ArrayUtils.reverse(bytes);

            int unsigned = unsignedToBytes(bytes[0]);
            freq[unsigned] += 1;

        }

        long min = Longs.min(freq);
        System.out.println("min = " + min);

        long max = Longs.max(freq);
        System.out.println("max = " + max);

        Assert.assertTrue(max - min <= 1);

    }

    public static int unsignedToBytes(byte b) {
        return b & 0xFF;
    }

}
