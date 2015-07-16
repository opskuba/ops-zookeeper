package com.opskuba.zookeeper.watcher;

import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AdminClient implements Watcher {

	private ZooKeeper zk;

	private String connnectHost;

	public AdminClient(String connnectHost) {
		this.connnectHost = connnectHost;
	}

	public void start() throws Exception {
		zk = new ZooKeeper(connnectHost, 15000, this);
	}

	public void listState() throws KeeperException, InterruptedException {
		try {
			Stat stat = new Stat();
			// master状态
			byte masterData[] = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime()); // master创建时间
			System.out.println("Master: " + new String(masterData) + " since " + startDate);
		} catch (KeeperException.NoNodeException e) {
			System.out.println("No Master");
		}
		// worker状态
		System.out.println("Workers:");
		for (String w : zk.getChildren("/workers", false)) {
			byte data[] = zk.getData("/workers/" + w, false, null);
			String state = new String(data);
			System.out.println("\t" + w + ": " + state);
		}
		// 任务分配状态
		System.out.println("Tasks:");
		for (String t : zk.getChildren("/assign", false)) {
			System.out.println("\t" + t);
		}
	}

	@Override
	public void process(WatchedEvent event) {

	}

	public static void main(String args[]) throws Exception {
		AdminClient c = new AdminClient("zookeeper.opskuba.com:2181");
		c.start();
		c.listState();
	}

}
