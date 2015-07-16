package com.opskuba.zookeeper.watcher;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 应用端负责提交任务到队列。
 * @author albert
 *
 */
public class Client implements Watcher {
	private ZooKeeper zk;

    private String connnectHost;

    public Client(String connnectHost) { this.connnectHost = connnectHost; }

    public void startZK() throws Exception {
        zk = new ZooKeeper(connnectHost, 15000, this);
    }

    public String queueCommand(String command) throws Exception {
        String name = null;
        while (true) {
            try {
                // 新节点名称
                name = zk.create("/tasks/task-",
                command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL); // 持久临时节点
                return name;
            } catch (KeeperException.NodeExistsException e) {
                throw new Exception(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {

    }

    public static void main(String args[]) throws Exception {
        Client c = new Client("zookeeper.opskuba.com:2181");
        c.startZK();
        String name = c.queueCommand("some cmd");
        System.out.println("Created " + name);
    }

}
