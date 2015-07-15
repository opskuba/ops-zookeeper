package com.opskuba.zookeeper.sample;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
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
	void runForMaster() throws KeeperException, InterruptedException {
		while (true) {
			try {
				zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				isLeader = true;
				break;
			} catch (NodeExistsException e) {
				isLeader = false;
				break;
			} catch (KeeperException.ConnectionLossException e) {

			}
			if (checkMaster())
				break;
		}
	}

	/**
	 * 检查master是否存在
	 * 
	 * @return 存在返回true，反之false
	 */
	boolean checkMaster() throws InterruptedException {
		while (true) {
			try {
				Stat stat = new Stat();
				byte data[] = zk.getData("/master", false, stat);
				isLeader = new String(data).equals(serverId);
				return true;
			} catch (NoNodeException e) {
				// 没有master节点，可返回false
				return false;
			} catch (KeeperException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		logger.info("->" + watchedEvent);
	}

	public static void main(String[] args) throws Exception {

		Master master = new Master("10.10.101.46:2181,10.10.102.46:2181,10.10.103.46:2181");

		master.startZooKeeper();
		master.runForMaster();

		if (master.isLeader) {
			System.out.println("I'm the Leader.");
		} else {
			System.out.println("I'm not the Leader.");
		}

		Thread.sleep(60000);

		master.stopZooKeeper();

	}

}
