package org.openstreetmap.osmosis.hbase.mr.analysis;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorImportFromWkb;

import java.nio.ByteBuffer;

/**
 * Created by willtemperley@gmail.com on 02-Nov-16.
 */
public class GeomIO<T extends Geometry> {

    private final Geometry.Type geomType;
    private OperatorImportFromWkb fromWkb = OperatorImportFromWkb.local();

    public GeomIO(Geometry.Type geomType) {
        this.geomType = geomType;
    }

    public T getGeometry(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        Geometry execute = fromWkb.execute(0, geomType, byteBuffer, null);
//        if (execute.getType().equals())
        return (T) execute;
    }

}
