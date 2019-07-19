package com.learn;


import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class WatchMoreTest {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(WatchMoreTest.class);
    //定义常量
    private static final String CONNECTSTRING = "192.168.1.105:2181";
    private static final String PATH = "/testNode3";
    private static final int SESSION_TIMEOUT = 50 * 1000;
    //定义实例变量
    private ZooKeeper zk = null;
    private String lastValue = "";

    //以下为业务方法
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, event -> {
        });
    }

    public void stopZK() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    public void createZNode(String path, String nodeValue) throws KeeperException, InterruptedException {
        zk.create(path, nodeValue.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String getZNode(String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, event -> {
            try {
                triggerValue(path);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }, new Stat());

        return new String(byteArray);
    }

    public boolean triggerValue(String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, event -> {
            try {
                triggerValue(path);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }, new Stat());

        String newValue = new String(byteArray);

        if (lastValue.equals(newValue)) {
            System.out.println("there is no change~~~~~~~~");
            return false;
        } else {
            System.out.println("lastValue: " + lastValue + "\t" + "newValue: " + newValue);
            this.lastValue = newValue;
            return true;
        }
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        WatchMoreTest watch = new WatchMoreTest();

        watch.setZk(watch.startZK());

        if (watch.getZk().exists(PATH, false) == null) {
            String initValue = "0000";
            watch.setLastValue(initValue);
            watch.createZNode(PATH, initValue);
            System.out.println("**********************>: " + watch.getZNode(PATH));
            Thread.sleep(Long.MAX_VALUE);
        } else {
            System.out.println("i have znode");
        }
    }


    //setter---getter
    public ZooKeeper getZk() {
        return zk;
    }

    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    public String getLastValue() {
        return lastValue;
    }

    public void setLastValue(String lastValue) {
        this.lastValue = lastValue;
    }


}