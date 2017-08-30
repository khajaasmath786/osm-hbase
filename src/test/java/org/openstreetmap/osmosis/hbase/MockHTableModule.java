package org.openstreetmap.osmosis.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.utility.MockHTableFactory;

/**
 * Mock table factory will be injected
 *
 * Created by willtemperley@gmail.com on 14-Jul-16.
 */
public class MockHTableModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TableFactory.class).to(MockHTableFactory.class).in(Singleton.class);
    }
}
