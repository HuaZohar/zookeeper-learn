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

/**
 * 启动程序后，手动在服务器上更改数据，更改第一次通知有效
 */
public class WatchOne {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(WatchOne.class);
    //定义常量
    private static final String CONNECTSTRING = "192.168.1.105:2181";
    private static final String PATH = "/testNode2";
    private static final int SESSION_TIMEOUT = 50 * 1000;
    //定义实例变量
    private ZooKeeper zk = null;

    //以下为业务方法
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
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
        byte[] byteArray = zk.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    triggerValue(path);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());
        return new String(byteArray);
    }

    public String triggerValue(String path) throws KeeperException, InterruptedException {
        byte[] byteArray = zk.getData(path, false, new Stat());
        String retValue = new String(byteArray);
        System.out.println("**************triggerValue: " + retValue);
        return retValue;
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        WatchOne watchOne = new WatchOne();

        watchOne.setZk(watchOne.startZK());

        if (watchOne.getZk().exists(PATH, false) == null) {
            watchOne.createZNode(PATH, "BBB");
            System.out.println("**********************>: " + watchOne.getZNode(PATH));
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

}