package com.zouxxyy.hbase.weibo.util;

import com.zouxxyy.hbase.weibo.constants.Constants;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 工具类
 * 1. 创建命名空间
 * 2. 判断表是否存在
 * 3. 创建表（3个）
 */
public class HBaseUtil {

    /**
     * 创建命名空间
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {

        // 建立连接
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        try {
            admin.createNamespace(namespaceDescriptor);
        }
        catch (NamespaceExistException e){
            System.out.println(nameSpace + " 命名空间存在!");
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    private static boolean isTableExist(String tableName) throws IOException {

        // 建立连接
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        // 关闭资源
        admin.close();
        connection.close();

        return exists;
    }

    /**
     * 建表
     * @param tableName
     * @param versions
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {

        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
            return;
        }

        // 建立连接
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        // 建表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf).setMaxVersions(versions)); // 设置版本
        }
        admin.createTable(hTableDescriptor);
        System.out.println("添加 " + tableName + " 表成功");

        // 关闭资源
        admin.close();
        connection.close();
    }

}
