// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.reader.HBaseReaderFactory;


/**
 * The plugin loader for the PostgreSQL Snapshot Schema tasks.
 * 
 * @author Brett Henderson
 */
public class HBasePluginLoader implements PluginLoader {


	private final Injector objectGraph;

	public HBasePluginLoader() throws IOException {
		objectGraph = Guice.createInjector(new TableModule());
	}


	private TaskManagerFactory getHBaseWriterFactory() {
		return objectGraph.getInstance(HBaseWriterFactory.class);
	}

	private TaskManagerFactory getHBaseChangeWriterFactory() {
		return objectGraph.getInstance(HBaseChangeWriterFactory.class);
	}

	private TaskManagerFactory getHBaseReaderFactory() {
		return objectGraph.getInstance(HBaseReaderFactory.class);
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, TaskManagerFactory> loadTaskFactories() {
		Map<String, TaskManagerFactory> factoryMap;

		factoryMap = new HashMap<String, TaskManagerFactory>();

		factoryMap.put("write-hbase-change", getHBaseChangeWriterFactory());
		factoryMap.put("whc", getHBaseChangeWriterFactory());
		factoryMap.put("write-hbase-change-0.6", getHBaseChangeWriterFactory());

		factoryMap.put("write-hbase", getHBaseWriterFactory());
		factoryMap.put("wh", getHBaseWriterFactory());
		factoryMap.put("write-hbase-0.6", getHBaseWriterFactory());

		factoryMap.put("read-hbase", getHBaseReaderFactory());
		factoryMap.put("rh", getHBaseReaderFactory());
		factoryMap.put("read-hbase-0.6", getHBaseReaderFactory());

		return factoryMap;
	}

}
