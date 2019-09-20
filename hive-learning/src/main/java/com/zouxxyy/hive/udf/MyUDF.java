package com.zouxxyy.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 自定义函数：一进一出
 */

public class MyUDF extends UDF {

    public int evaluate(int data) {

        return data + 5;
    }

    // 支持重载
    public int evaluate(int data, int data2) {

        return data + data2 + 5;
    }
}
