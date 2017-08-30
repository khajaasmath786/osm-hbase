package org.openstreetmap.osmosis.hbase.extract;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.List;

/**
 * Created by willtemperley@gmail.com on 30-Aug-16.
 */
public class InclusionCriterion {

    String key;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    List<String> values;

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    String polygon;

    public String getPolygon() {
        return polygon;
    }

    private boolean all = false;
    private boolean whitelist = false;

    public void setPolygon(String polygon) {
        this.polygon = polygon;

        if (polygon.equals("all")) {
            all = true;
        } else if (polygon.equals("whitelist")) {
            whitelist = true;
        } else if (polygon.equals("blacklist")) {
            whitelist = false;
        }
    }


    public boolean isPolygon(Way way) {

        /*
        They will be:
        - all OK
        - whitelisted
        - blacklisted tf (i.e contained but blacklisted)
         */
        for (Tag tag : way.getTags()) {
            String tagKey = tag.getKey();
            String tagVal = tag.getValue();

            if (tagKey.equals(key)) {
                return all || values.contains(tagVal) && whitelist;

            }
        }

        return false;
    }
}

