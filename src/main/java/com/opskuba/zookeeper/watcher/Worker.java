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
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Watcher {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private ZooKeeper zk;

	private String connnectHost;

	private Random random = new Random();

	private String serverId = Integer.toHexString(random.nextInt());

	private String status;

	private String name;

	public Worker(String connnectHost) {
		this.connnectHost = connnectHost;
	}

	public void startZK() throws IOException {
		zk = new ZooKeeper(connnectHost, 15000, this);
	}

	@Override
	public void process(WatchedEvent event) {
		logger.info(event.toString() + ", " + connnectHost);
	}

	/**
	 * 注册自己为Worker
	 */
	public void register() {
		name = "worker-" + serverId;
		zk.create("/workers/worker-" + name, // worker标识
				"Idle".getBytes(), // 状态空闲
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, // 临时节点
				createWorkerCallback, null);
	}

	private AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (KeeperException.Code.get(rc)) {
			case CONNECTIONLOSS:
				register();
				break;
			case OK:
				logger.info("Registered successfully: " + serverId);
				break;
			case NODEEXISTS:
				logger.warn("Already registered: " + serverId);
				break;
			default:
				logger.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
			}

		}
	};

	private AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (KeeperException.Code.get(rc)) {
			case CONNECTIONLOSS:
				updateStatus((String) ctx);
				return;
			default:
				break;
			}
		}
	};

	synchronized private void updateStatus(String status) {
		if (status == this.status) {
			zk.setData("/workers/" + name, status.getBytes(), -1, // -1将不会检查版本
					statusUpdateCallback, status);
		}
	}

	/**
	 * 设置状态
	 * 
	 * @param status
	 *            新状态
	 */
	public void setStatus(String status) {
		this.status = status;
		updateStatus(status);
	}

	public static void main(String args[]) throws Exception {
		Worker w = new Worker("zookeeper.opskuba.com:2181");
		w.startZK();
		w.register();
		Thread.sleep(60000);
	}
}
