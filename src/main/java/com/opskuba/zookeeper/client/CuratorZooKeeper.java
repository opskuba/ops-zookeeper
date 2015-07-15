package com.opskuba.zookeeper.client;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * 基于Curator库测试Zookeeper.
 * 
 * @author albert
 *
 */
public class CuratorZooKeeper {

	public static void main(String[] args) {

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		// CuratorFramework client = CuratorFrameworkFactory.newClient(
		// "127.0.0.1:2181", 5000, 3000, retryPolicy);
		CuratorFramework client = CuratorFrameworkFactory.builder().connectString("10.10.101.46:2181")
				.sessionTimeoutMs(5000).connectionTimeoutMs(3000).retryPolicy(retryPolicy).build();

		client.start();

		try {
			Stat stat = client.checkExists().forPath("/uplus");

			if (null != stat) {
				client.delete().deletingChildrenIfNeeded().forPath("/uplus");
			}

			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
					.forPath("/uplus", "whereareyou?".getBytes());
			client.setData().forPath("/uplus", "newalbert".getBytes());
			// client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/albert","whoareyou".getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
