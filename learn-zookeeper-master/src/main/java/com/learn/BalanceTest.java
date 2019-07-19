package com.learn;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BalanceTest {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(BalanceTest.class);
    //定义常量
    private static final String CONNECTSTRING = "192.168.1.105:2193";
    private static final int SESSION_TIMEOUT = 50 * 1000;
    private static final String PATH = "/testNode99";
    private static final String SUB_PREFIX = "sub";
    //定义实例变量
    private ZooKeeper zk = null;
    private int subCount = 5;
    private List<String> serviceNodeLists = new ArrayList<String>();
    private int serviceNum = 0;


    //以下为业务方法
    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    serviceNodeLists = zk.getChildren(PATH, true);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public String dealRequest() throws KeeperException, InterruptedException {
        serviceNum = serviceNum + 1;

        for (int i = serviceNum; i <= subCount; i++) {
            if (serviceNodeLists.contains(SUB_PREFIX + serviceNum)) {
                return new String(zk.getData(PATH + "/" + SUB_PREFIX + serviceNum, false, new Stat()));
            } else {
                serviceNum = serviceNum + 1;
            }
        }
        for (int i = 1; i <= subCount; i++) {
            if (serviceNodeLists.contains(SUB_PREFIX + i)) {
                serviceNum = i;
                return new String(zk.getData(PATH + "/" + SUB_PREFIX + serviceNum, false, new Stat()));
            }
        }
        return "null node~~~~~";
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        BalanceTest test = new BalanceTest();

        test.setZk(test.startZK());
        Thread.sleep(3000);
        String result = null;
        //以轮询的方式访问15次，共计5个节点来应付实现负载均衡
        for (int i = 1; i <= 15; i++) {
            result = test.dealRequest();
            System.out.println("****loop:" + i + "\t" + test.serviceNum + "\t" + result);
            Thread.sleep(2000);
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