package org.openstreetmap.osmosis.hbase.extract;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 30-Aug-16.
 */
public class PolygonDetector {

    public static List<InclusionCriterion> inclusionCriteria = new ArrayList<InclusionCriterion>();

    static {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        InputStream resourceAsStream = contextClassLoader.getResourceAsStream("polygon-features.json");

        TypeReference<List<InclusionCriterion>> mapType = new TypeReference<List<InclusionCriterion>>() {};

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            List<InclusionCriterion> obj = objectMapper.readValue(resourceAsStream, mapType);
            inclusionCriteria.addAll(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static boolean isWayPolygon(Way way) {

        List<WayNode> wayNodes = way.getWayNodes();

        //closed?
        if (wayNodes.get(0).getNodeId() != wayNodes.get(wayNodes.size()-1).getNodeId()) {
            return false;
        }

        //Todo put the criteria in a map based on tag key?
        for (InclusionCriterion inclusionCriterion : inclusionCriteria) {
            if (inclusionCriterion.isPolygon(way)) {
                return true;
            }
        }

        return false;
    }

}
