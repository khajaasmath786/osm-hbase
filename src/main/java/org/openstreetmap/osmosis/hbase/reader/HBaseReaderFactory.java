package org.openstreetmap.osmosis.hbase.reader;

import com.google.inject.Inject;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.RunnableDatasetSourceManager;

/**
 * Just creates a reader
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public class HBaseReaderFactory extends TaskManagerFactory {

    private final HBaseReader hbaseReader;

    @Inject
    public HBaseReaderFactory(HBaseReader hBaseReader) {
        this.hbaseReader = hBaseReader;
    }

    @Override
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {

        return new RunnableDatasetSourceManager(
                taskConfig.getId(),
                hbaseReader,
                taskConfig.getPipeArgs()
        );
    }
}
