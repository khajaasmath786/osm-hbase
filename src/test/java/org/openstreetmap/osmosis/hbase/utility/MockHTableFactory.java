package org.openstreetmap.osmosis.hbase.utility;


import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by willtemperley@gmail.com on 13-Jul-16.
 */
public class MockHTableFactory implements TableFactory {

    private static final Logger logger = LoggerFactory.getLogger(MockHTableFactory.class);

    public MockHTableFactory() {
        logger.info("Creating mock table factory");
    }

    private Map<String, Table> tables = new HashMap<String, Table>();

    /**
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    @Override
    public Table getTable(String tableName) throws IOException {
        if (tables.containsKey(tableName)) {
            return tables.get(tableName);
        }
        MockHTable table = new MockHTable(tableName, "d", "t");
        tables.put(tableName, table);
        return table;
    }


    @Override
    public void close() {

    }

    @Override
    public Connection getConnection() {
        throw new NotImplementedException();
    }

}
