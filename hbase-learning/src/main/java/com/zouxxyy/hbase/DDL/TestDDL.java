package com.zouxxyy.hbase.DDL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * DDL测试
 */

public class TestDDL {

    private Admin admin;
    private Connection connection;

    @Before
    public void before() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * 创建表测试
     */
    @Test
    public void createTable() throws IOException {

        String tableName = "zxylearn:student1";
        String[] cfs = {"info1", "info2"};

        if(admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " 表已经存在！");
        }
        else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 添加列族
            for (String cf : cfs) {
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(hTableDescriptor);
            System.out.println("添加 " + tableName + " 表成功");
        }
    }

    /**
     * 删除表测试
     */
    @Test
    public void dropTable() throws IOException {
        String tableName = "zxylearn:student1";

        if(!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " 表不存在！");
        }
        else {
            // 使表下线
            admin.disableTable(TableName.valueOf(tableName));

            // 使表删除
            admin.deleteTable(TableName.valueOf(tableName));

            if(!admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println(tableName + " 表已删除！");
            }
        }
    }

    /**
     * 创建命名空间
     */

    @Test
    public void createNameSpace() {
        String ns = "ns1";

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        try {
            admin.createNamespace(namespaceDescriptor);
        }
        catch (NamespaceExistException e){
            System.out.println(ns + " 命名空间存在!");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() throws IOException {
        admin.close();
        connection.close();
    }
}
