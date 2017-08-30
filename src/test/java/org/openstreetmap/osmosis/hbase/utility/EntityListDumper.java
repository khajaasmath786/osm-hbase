// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.hbase.utility;

import org.openstreetmap.osmosis.core.container.v0_6.Dataset;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.task.v0_6.DatasetSinkSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.util.List;


/**
 * Reads all data from a dataset.
 * 
 * @author Brett Henderson
 */
public class EntityListDumper implements DatasetSinkSource {

	private Sink sink;

    private List<EntityContainer> entities;

	public EntityListDumper(List<EntityContainer> entities) {
		this.entities = entities;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setSink(Sink sink) {
		this.sink = sink;
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void process(Dataset dataset) {

		// Pass all data within the dataset to the sink.
		for (EntityContainer entity : entities) {
			sink.process(entity);
		}

		sink.complete();
			
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release() {
		sink.release();
	}
}
