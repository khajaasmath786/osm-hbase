package org.openstreetmap.osmosis.hbase.reader;

import com.google.inject.Inject;
import org.openstreetmap.osmosis.core.container.v0_6.Dataset;
import org.openstreetmap.osmosis.core.container.v0_6.DatasetContext;
import org.openstreetmap.osmosis.core.task.v0_6.DatasetSink;
import org.openstreetmap.osmosis.core.task.v0_6.DatasetSource;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableDatasetSource;
import org.openstreetmap.osmosis.hbase.common.TableFactory;

/**
 *
 * Created by willtemperley@gmail.com on 15-Jul-16.
 */
public class HBaseReader implements RunnableDatasetSource, Dataset {

    private final TableFactory tableFactory;
    private DatasetSink datasetSink;

    @Inject
    public HBaseReader(TableFactory tableFactory){
        this.tableFactory = tableFactory;
    }


    @Override
    public void run() {
        try {
            datasetSink.process(this);

        } finally {
            datasetSink.release();
        }

    }

    @Override
    public void setDatasetSink(DatasetSink datasetSink) {

        this.datasetSink = datasetSink;
    }

    @Override
    public DatasetContext createReader() {
        return new HBaseDatasetContext(tableFactory);
    }
}
