package com.zouxxyy.hbase.weibo.dao;

import com.zouxxyy.hbase.weibo.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 业务相关的操作
 * 1. 发布微博
 * 2. 删除微博(没写)
 * 3. 关注用户
 * 4. 取关用户
 * 5. 获取用户微博详情
 * 6. 获取用户的初始化页面
 */
public class HBaseDao {

    /**
     * 发布微博
     * @param uid
     * @param content
     * @throws IOException
     */
    public static void publishWeiBo(String uid, String content) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分：操作微博内容表

        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        String rowKey = uid + "_" + System.currentTimeMillis();

        Put contentPut = new Put(Bytes.toBytes(rowKey)); // RowKey

        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        contentTable.put(contentPut);

        // 第二部分：操作微博收件箱表

        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 获取发布者的全部粉丝，只需要粉丝列族
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relaTable.get(get);

        // 构建收件箱表的Put对象集合
        ArrayList<Put> inboxPuts = new ArrayList<>();
        for (Cell cell : result.rawCells()) {

            // 构建Put对象(要带数据)，再加入到集合中
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));

            inboxPuts.add(inboxPut);
        }

        // 提交Put对象集合
        if(inboxPuts.size() > 0) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);
            inboxTable.close();
        }

        relaTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 关注用户
     * @param uid
     * @param attends
     * @throws IOException
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        if (attends.length <= 0) {
            System.out.println("请选择待关注的人!");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分：操作用户关系表

        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Put> relaPuts = new ArrayList<>();

        // 构建关注者的Put对象
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            // 构建粉丝的Put对象
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relaPuts.add(attendPut);
        }
        relaPuts.add(uidPut);

        relaTable.put(relaPuts);


        // 第二部分：操作收件箱表

        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Put inboxPut = new Put(Bytes.toBytes(uid));

        // 循环关注者
        for (String attend : attends) {

            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));

            ResultScanner resultScanner = contentTable.getScanner(scan);

            // 保证时间戳不同
            long ts = System.currentTimeMillis();

            // 获取该关注者的近期微博
            for (Result result : resultScanner) {

                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow()); // getRow得到的是RowKey

            }
        }

        if (!inboxPut.isEmpty()) {

            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            inboxTable.put(inboxPut);

            inboxTable.close();
        }

        relaTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 取关用户
     * @param uid
     * @param dels
     * @throws IOException
     */
    public static void deleteAttends(String uid, String... dels) throws IOException {

        if (dels.length <= 0) {
            System.out.println("请选择待取关的人!");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分：操作用户关系表

        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Delete> relaDeletes = new ArrayList<>();

        // 构建关注者的Delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {

            // 注意用的是addColumns
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));

            // 构建粉丝的Delete对象
            Delete delDelete = new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            relaDeletes.add(delDelete);
        }
        relaDeletes.add(uidDelete);

        relaTable.delete(relaDeletes);


        // 第二部分：操作收件箱表

        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        // 删除关注者列
        for (String del : dels) {

            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));

        }
        inboxTable.delete(inboxDelete);

        inboxTable.close();
        relaTable.close();
        connection.close();

    }

    /**
     * 获取某个用户的初始化界面(他关注的用户的最近3条微博)
     * @param uid
     * @throws IOException
     */
    public static void getInit(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);

        for (Cell cell : result.rawCells()) {

            // 构建微博内容表的get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            Result contentResult = contentTable.get(contentGet);

            // 打印微博内容
            for (Cell rawCell : contentResult.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(rawCell)) +
                        ", 列族: " + Bytes.toString(CellUtil.cloneFamily(rawCell)) +
                        ", 列名: " + Bytes.toString(CellUtil.cloneQualifier(rawCell)) +
                        ", 值: " + Bytes.toString(CellUtil.cloneValue(rawCell)));
            }
        }
        inboxTable.close();
        contentTable.close();
        connection.close();
    }


    /**
     * 获取某个用户的全部微博
     * @param uid
     * @throws IOException
     */
    public static void getWeiBo(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Scan scan = new Scan();

        // 构建过滤器(取包含"uid_"的RowKey)
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);

        ResultScanner scanner = contentTable.getScanner(scan);

        // 打印数据
        for (Result result : scanner) {
            for (Cell rawCell : result.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(rawCell)) +
                        ", 列族: " + Bytes.toString(CellUtil.cloneFamily(rawCell)) +
                        ", 列名: " + Bytes.toString(CellUtil.cloneQualifier(rawCell)) +
                        ", 值: " + Bytes.toString(CellUtil.cloneValue(rawCell)));
            }
        }

        contentTable.close();
        connection.close();

    }

}
