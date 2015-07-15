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

/**
 * 注册为master.
 * 
 * @author albert
 *
 */
public class Master implements Watcher {

	private Logger logger = LoggerFactory.getLogger(getClass());

	ZooKeeper zk;

	String serverId = Integer.toHexString(new Random().nextInt());

	boolean isLeader = false;

	private final String hostPort;

	public Master(String hostPort) {
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

	/**
	 * 抢占式，向zookeeper注册为master节点.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	void runForMaster() {
		zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
				new AsyncCallback.StringCallback() {
					@Override
					public void processResult(int rc, String path, Object ctx, String name) {
						switch (KeeperException.Code.get(rc)) {
						case CONNECTIONLOSS:
							checkMaster();
							return;
						case OK: // 创建节点成功，成功获取领导权
							isLeader = true;
							break;
						default:
							isLeader = false;
						}
					logger.info("I'm " + (isLeader ? "" : "not ") + "the leader.");
					}
				}, null);
	}

	/**
	 * 检查master是否存在
	 * 
	 * @return 存在返回true，反之false
	 */
	private void checkMaster() {
		zk.getData("/master", false, new AsyncCallback.DataCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
				case CONNECTIONLOSS:
					checkMaster();
					return;
				case NONODE: // 没有master节点存在，则尝试获取领导权
					runForMaster();
					return;
				case NODEEXISTS:
					System.out.println("node exists.");
				default:
					break;
				}
			}
		}, null);
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		logger.info("->" + watchedEvent);
	}

	public static void main(String[] args) throws Exception {

		Master master = new Master("zookeeper.opskuba.com:2181");

		master.startZooKeeper();
		master.runForMaster();

		Thread.sleep(60000);

		master.stopZooKeeper();

	}

}
