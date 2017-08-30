package org.openstreetmap.osmosis.hbase.common;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public interface TableFactory {

    Table getTable(String tableName) throws IOException;

    void close();

    Connection getConnection();
}
