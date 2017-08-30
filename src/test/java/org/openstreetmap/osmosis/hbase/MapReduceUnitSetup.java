package org.openstreetmap.osmosis.hbase;

import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.testutil.AbstractDataTest;

/**
 * Created by willtemperley@gmail.com on 28-Oct-16.
 */
public class MapReduceUnitSetup extends AbstractDataTest {


    protected Injector injector;

    /*
            On a cluster this would already be set.
             */
    protected void setupSerialization(MapReduceDriver<?, ?, ?, ?, ?, ?> mapReduceDriver) {
        Configuration configuration = mapReduceDriver.getConfiguration();
        configuration.setStrings("io.serializations", configuration.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());
    }

    protected TableFactory getTableFactory() {
        return injector.getInstance(TableFactory.class);
    }
}
