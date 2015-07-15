package com.opskuba.zookeeper.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void process(WatchedEvent event) {

		logger.info("<-" + event);

	}

}
