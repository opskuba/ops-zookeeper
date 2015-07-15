package com.opskuba.zookeeper.watcher;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap implements Watcher {

	private Logger logger = LoggerFactory.getLogger(getClass());

	ZooKeeper zk;

	String serverId = Integer.toHexString(new Random().nextInt());

	private final String hostPort;

	public Bootstrap(String hostPort) {
		this.hostPort = hostPort;
	}

	void startZooKeeper() {
		try {
			zk = new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			logger.error("初始化zookeeper失败" + e.getMessage());
			throw new RuntimeException("初始化zookeeper失败", e);
		}
	}

	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}

	private void createParent(String path, byte[] data) {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, // 持久节点
				createParentCb, data); // 将data传给ctx，这样可以在Callback中继续尝试创建节点
	}

	private AsyncCallback.StringCallback createParentCb = new AsyncCallback.StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (KeeperException.Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[]) ctx); // 取出data
				break;
			case OK:
				logger.info("Parent created");
				break;
			case NODEEXISTS:
				logger.info("Parent already registered: " + path);
				break;
			default:
				logger.info("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
			}
		}
	};

	@Override
	public void process(WatchedEvent event) {
		logger.info("" + event);
	}

	/**
	 * 客户端主动断开。
	 */
	void stopZooKeeper() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			logger.error("zookeeper关闭失败" + e.getMessage());
			throw new RuntimeException("zookeeper关闭失败", e);
		}
	}

	public static void main(String[] args) throws InterruptedException {

		Bootstrap bootstrap = new Bootstrap("zookeeper.opskuba.com:2181");
		bootstrap.startZooKeeper();
		bootstrap.bootstrap();
		Thread.sleep(60000);

		bootstrap.stopZooKeeper();

	}

}
