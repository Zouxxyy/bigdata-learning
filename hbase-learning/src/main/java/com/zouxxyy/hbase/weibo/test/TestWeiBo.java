package com.zouxxyy.hbase.weibo.test;

import com.zouxxyy.hbase.weibo.constants.Constants;
import com.zouxxyy.hbase.weibo.dao.HBaseDao;
import com.zouxxyy.hbase.weibo.util.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init() {

        try {
            // 创建命名空间
            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            // 创建3个表
            HBaseUtil.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS, Constants.CONTENT_TABLE_CF);
            HBaseUtil.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSIONS, Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);
            HBaseUtil.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONS, Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        init();

        // 1001 发布微博
        HBaseDao.publishWeiBo("1001", "国庆假期结束，你好啊！");
        Thread.sleep(10);

        // 1002关注1001 和 1003
        HBaseDao.addAttends("1002", "1001", "1003");

        // 获取1002的初始化界面
        HBaseDao.getInit("1002");
        System.out.println("**************111*************");

        // 1003 发布3条微博，同时1001发布2条微博
        HBaseDao.publishWeiBo("1003", "加油！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1001", "中国加油！！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1003", "继续学习！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1001", "啦啦啦！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1003", "b站干杯！");
        Thread.sleep(10);

        // 获取1002的初始化界面
        HBaseDao.getInit("1002");
        System.out.println("**************222*************");

        // 1002取关1003
        HBaseDao.deleteAttends("1002", "1003");

        // 获取1002的初始化界面
        HBaseDao.getInit("1002");
        System.out.println("**************333*************");

        // 1002再次关注1003
        HBaseDao.addAttends("1002", "1003");

        // 获取1002的初始化界面
        HBaseDao.getInit("1002");
        System.out.println("**************444*************");

        // 获取1001微博详情
        HBaseDao.getWeiBo("1001");

    }
}

/*
// 控制台结果
添加 weibo:content 表成功
添加 weibo:relation 表成功
添加 weibo:inbox 表成功
RowKey: 1001_1570696460048, 列族: info, 列名: content, 值: 国庆假期结束，你好啊！
**************111*************
RowKey: 1001_1570696461440, 列族: info, 列名: content, 值: 啦啦啦！
RowKey: 1001_1570696460254, 列族: info, 列名: content, 值: 中国加油！！
RowKey: 1003_1570696461485, 列族: info, 列名: content, 值: b站干杯！
RowKey: 1003_1570696460296, 列族: info, 列名: content, 值: 继续学习！
**************222*************
RowKey: 1001_1570696461440, 列族: info, 列名: content, 值: 啦啦啦！
RowKey: 1001_1570696460254, 列族: info, 列名: content, 值: 中国加油！！
**************333*************
RowKey: 1001_1570696461440, 列族: info, 列名: content, 值: 啦啦啦！
RowKey: 1001_1570696460254, 列族: info, 列名: content, 值: 中国加油！！
RowKey: 1003_1570696461485, 列族: info, 列名: content, 值: b站干杯！
RowKey: 1003_1570696460296, 列族: info, 列名: content, 值: 继续学习！
**************444*************
RowKey: 1001_1570696460048, 列族: info, 列名: content, 值: 国庆假期结束，你好啊！
RowKey: 1001_1570696460254, 列族: info, 列名: content, 值: 中国加油！！
RowKey: 1001_1570696461440, 列族: info, 列名: content, 值: 啦啦啦！

// hbase shell 打印结果
hbase(main):001:0> scan 'weibo:relation'
ROW                     COLUMN+CELL
 1001                   column=fans:1002, timestamp=1570696460157, value=1002
 1002                   column=attends:1001, timestamp=1570696460157, value=1001
 1002                   column=attends:1003, timestamp=1570696463829, value=1003
 1003                   column=fans:1002, timestamp=1570696463829, value=1002

 */