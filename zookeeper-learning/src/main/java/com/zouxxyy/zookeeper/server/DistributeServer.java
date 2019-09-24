package com.zouxxyy.zookeeper.server;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 服务器节点动态上下限案例
 * 服务端：在zk中注册节点上线
 */

public class DistributeServer {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000; // 失联时间

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeServer server = new DistributeServer();

        // 1. 连接zkCli
        server.getConnect();

        // 2. 注册节点
        server.register("localhost");

        // 3. 业务逻辑
        server.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void register(String hostname) throws KeeperException, InterruptedException {
        // -e -s 模式
        String path = zkCli.create("/servers/server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + "上线");
    }


    private void getConnect() throws IOException {
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, e -> {
            System.out.println("默认回调函数");
        });
    }

}
