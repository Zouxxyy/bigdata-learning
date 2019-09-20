package com.zouxxyy.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义函数：一进多出
 */

public class MyUDTF extends GenericUDTF {

    private List<String> dataList = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 可以多字段输出，这里只设置一个字段
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word"); // 字段默认名字(列名)

        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); // 输出数据的类型

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    public void process(Object[] args) throws HiveException {

        // 1. 获取参数（函数cans）
        String data = args[0].toString();
        String splitKey = args[1].toString();

        // 2. 切分(业务逻辑)
        String[] words = data.split(splitKey);

        // 3. 写出数据
        for (String word : words) {

            // 4. 返回的必须的集合，因为初始化时返回的字段是数组
            dataList.clear();
            dataList.add(word);
            forward(dataList);
        }
    }

    public void close() throws HiveException {

    }
}
