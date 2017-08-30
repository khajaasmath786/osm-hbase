package org.openstreetmap.osmosis.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.openstreetmap.osmosis.hbase.common.TableFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads and wraps an hbase connection
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public class HTableFactory  implements TableFactory {

    private final Connection connection;

    public HTableFactory() throws IOException {
        connection = ConnectionFactory.createConnection(getConfiguration());
    }

    public Connection getConnection() {
        return connection;
    }

    private Configuration getConfiguration() throws IOException {
        Configuration configuration = new Configuration();
        Properties props = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream("hbase-config.properties");

        props.load(resourceStream);

        String quorum = "hbase.zookeeper.quorum";
        String master = "hbase.master";
        configuration.set(quorum, props.getProperty(quorum));
        configuration.set(master, props.getProperty(master));
        return configuration;
    }

    @Override
    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
