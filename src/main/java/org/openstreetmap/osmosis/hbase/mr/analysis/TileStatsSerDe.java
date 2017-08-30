package org.openstreetmap.osmosis.hbase.mr.analysis;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by willtemperley@gmail.com on 20-Mar-17.
 */
public class TileStatsSerDe {

    public static byte[] cfD = Bytes.toBytes("d"); //ImageData
    public static byte[] distCol = Bytes.toBytes(-9999);

    public void serialize(int[] ints) {

    }

    public Double deSerializeOSM(Result result) {

        byte[] value = result.getValue(cfD, distCol);
        if (value == null)  {
            return 0d;
        }
        return Bytes.toDouble(value);
    }

    public Map<Integer, Integer> deSerialize(Result result) {
        Map<Integer, Integer> map = new HashMap<>();
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(cfD);

        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            int kI = Bytes.toInt(key);
            if (kI == -9999) {
                continue;
            }
            int kV = Bytes.toInt(value);
            map.put(kI, kV);
        }
        return map;
    }

}
