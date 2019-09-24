package com.zouxxyy.zookeeper.api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * zk 的 api 测试
 */

public class ZkClient {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000; // 失联时间

    @Before
    public void before() throws IOException {
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, e -> {
            System.out.println("默认回调函数");
        });
    }

    // ls 测试
    @Test
    public void ls() throws KeeperException, InterruptedException {

        // 参数有：路径，（可选择是否用默认回调函数 或 自定义回调函数)
        List<String> children = zkCli.getChildren("/", e -> {
            System.out.println("数据改变了");
        });

        System.out.println("=================================");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("=================================");

        Thread.sleep(Long.MAX_VALUE);
    }

    // create 测试
    @Test
    public void create() throws KeeperException, InterruptedException {

        // 参数有：路径，数据， ACL，模式(-s,-e)
        String s = zkCli.create("/zxy", "zxy2019".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println(s);

        Thread.sleep(Long.MAX_VALUE);
    }

    // get 测试
    @Test
    public void get() throws KeeperException, InterruptedException {

        Stat stat = new Stat();

        // stat参数 用于得到节点status
        byte[] data = zkCli.getData("/zxy", true, stat);

        System.out.println(new String(data));
        System.out.println(stat.toString());
    }

    // set 测试
    @Test
    public void set() throws KeeperException, InterruptedException {

        Stat exists = zkCli.exists("/zxy", false);

        // set前先判断节点是否存在，并获取版本号
        if (exists == null) {
            System.out.println("节点不存在");
        }
        else {
            System.out.println("修改前节点版本为：" + exists.getVersion());
            Stat stat = zkCli.setData("/zxy", "123456".getBytes(), exists.getVersion());
            System.out.println("修改后节点版本为：" + stat.getVersion());
        }
    }

    // delete 测试
    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/zxy", false);

        // delete前先判断节点是否存在，并获取版本号
        if (exists == null) {
            System.out.println("节点不存在");
        }

        else {
            zkCli.delete("/zxy", exists.getVersion());
            System.out.println("删除成功");
        }
    }

    // 循环调用
    public void register() throws KeeperException, InterruptedException{
        byte[] data = zkCli.getData("/zxy", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    register();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);

        System.out.println(new String(data));
    }

    @Test
    public void testRegister() throws KeeperException, InterruptedException {
        register();
        Thread.sleep(Long.MAX_VALUE);
    }

}
