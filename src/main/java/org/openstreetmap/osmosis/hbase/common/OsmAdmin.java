package org.openstreetmap.osmosis.hbase.common;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.openstreetmap.osmosis.hbase.TableModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Admin actions for database
 *
 * Created by willtemperley@gmail.com on 14-Oct-16.
 */
public class OsmAdmin {

    public static void main(String[] args) throws IOException {

        Injector objectGraph = Guice.createInjector(new TableModule());
        TableFactory tableFactory = objectGraph.getInstance(TableFactory.class);

        List<TableName> tableNames = new ArrayList<TableName>();

        tableNames.add(TableName.valueOf(Tables.ways.toString()));
        tableNames.add(TableName.valueOf(Tables.nodes.toString()));
        tableNames.add(TableName.valueOf(Tables.relations.toString()));

        String action = args[0];
        if (action.equals("truncateAll")) {
            System.out.println("action = " + action);
            recreate(tableFactory.getConnection(), tableNames);
        } else {
            System.out.println("Unknown action: " + action);
        }

    }

    public static void recreate(Connection connection, List<TableName> tableNames) throws IOException {
        Admin admin = connection.getAdmin();

        for (TableName tableName : tableNames) {
            System.out.println("truncating: " + tableName);
            admin.disableTable(tableName);
            admin.truncateTable(tableName, true);
        }

    }
}
