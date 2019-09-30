package com.zouxxyy.hbase.DML;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * DML测试
 */

public class TestDML {

    private Table table;
    private Connection connection;

    @Before
    public void before() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("zxylearn:student"));
    }

    /**
     * 向表中插入数据
     */
    @Test
    public void putData() throws IOException {

        Put put = new Put(Bytes.toBytes("1007")); // RowKey
        put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("addr"), Bytes.toBytes("hubei"));

        table.put(put);
    }

    /**
     * 获取数据(get)
     */
    @Test
    public void getData() throws IOException {

        Get get = new Get(Bytes.toBytes("1007"));
        get.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("addr"));

        Result result = table.get(get);
        // 解析并打印result
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("列族: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", 列名: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", 值: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 获取数据(scan)
     */
    @Test
    public void scanTable() throws IOException {

        // 构建scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1005"));

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            // 解析并打印result
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                        "列族: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", 列名: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", 值: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }







    @After
    public void after() throws IOException {
        table.close();
        connection.close();
    }
}
