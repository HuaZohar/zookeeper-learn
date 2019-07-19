package com.learn;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class TestZookeeper {


    private static final String CONNECT_STRING = "192.168.1.105:2181";
    private static final String PATH = "/atguigu";
    private static final int SESSION_TIMEOUT = 50 * 1000;


    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    public void stopZK(ZooKeeper zk) throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    public void createZNode(ZooKeeper zk, String path, String nodeValue) throws KeeperException, InterruptedException {
        zk.create(path, nodeValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String getZNode(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, false, new Stat());
        return new String(byteArray);
    }

    public static void main(String[] args) {

        TestZookeeper testZookeeper = new TestZookeeper();

        try {
            final ZooKeeper zooKeeper = testZookeeper.startZK();


            final Stat stat = zooKeeper.exists(PATH, false);

            if (stat == null) {
                testZookeeper.createZNode(zooKeeper, PATH, "zk1014");
                String result = testZookeeper.getZNode(zooKeeper, PATH);
                System.out.println("**********result: " + result);
            } else {
                System.out.println("***********znode has already ok***********");
            }

            testZookeeper.stopZK(zooKeeper);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
