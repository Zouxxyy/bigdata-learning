package com.zouxxyy.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务器节点动态上下线案例
 * 客户端：连续监听zk中上线的服务器
 */

public class DistributeClient {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000; // 失联时间

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        // 1. 连接zkCli
        client.getConnect();

        // 2. 连续监听
        client.getChildren();

        // 3. 业务逻辑
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {

        // 循环监听
        List<String> children = zkCli.getChildren("/servers", event -> {
            try {
                getChildren();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        ArrayList<String> hosts = new ArrayList<>();

        for (String child : children) {
            byte[] data = zkCli.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        System.out.println(hosts);
    }

    private void getConnect() throws IOException {
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, e -> {
            System.out.println("默认回调函数");
        });
    }
}
