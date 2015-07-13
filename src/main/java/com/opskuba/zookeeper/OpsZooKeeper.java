package com.opskuba.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 基于Curator实现Zookeeper
 */
public class OpsZooKeeper {

	public static void main(String[] args) {
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		// CuratorFramework client = CuratorFrameworkFactory.newClient(
		// "127.0.0.1:2181", 5000, 3000, retryPolicy);
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString("127.0.0.1:2181")
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(retryPolicy).build();
		
		client.start();
	}

}
