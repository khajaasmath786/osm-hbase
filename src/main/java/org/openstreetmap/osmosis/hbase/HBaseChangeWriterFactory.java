// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.hbase;

import com.google.inject.Inject;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.ChangeSinkManager;


/**
 * The task manager factory for a hbase change writer.
 *
 * @author willtemperley@gmail.com
 */
public class HBaseChangeWriterFactory extends TaskManagerFactory {
	
	private static final String ARG_KEEP_INVALID_WAYS = "keepInvalidWays";
	private static final boolean DEFAULT_KEEP_INVALID_WAYS = true;

	private final HBaseWriter changeWriter;

    @Inject
	public HBaseChangeWriterFactory(HBaseWriter changeWriter){
    	this.changeWriter = changeWriter;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {

		boolean keepInvalidWays = getBooleanArgument(taskConfig, ARG_KEEP_INVALID_WAYS, DEFAULT_KEEP_INVALID_WAYS);

		return new ChangeSinkManager(
			taskConfig.getId(),
            changeWriter,
			taskConfig.getPipeArgs()
		);
	}

}
