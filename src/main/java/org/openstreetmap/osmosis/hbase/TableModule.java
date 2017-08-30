package org.openstreetmap.osmosis.hbase;


import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.openstreetmap.osmosis.hbase.common.TableFactory;

import java.io.IOException;

/**
 * Determines the implementation of TableFactory
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public class TableModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(TableFactory.class).to(HTableFactory.class).in(Singleton.class);
    }
}
