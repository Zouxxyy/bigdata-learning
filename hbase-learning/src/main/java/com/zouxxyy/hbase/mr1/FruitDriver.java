package com.zouxxyy.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {

    private Configuration conf = null;

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new FruitDriver(), args);

        System.exit(run);
    }


    @Override
    public int run(String[] args) throws Exception {

        // 1 获取Job对象
        Job job = Job.getInstance(conf);

        // 2 设置类路径
        job.setJarByClass(FruitDriver.class);

        // 3 设置Mapper
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 4 设置Reducer
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);

        // 5. 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 6. 提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b  ? 0 : 1);

        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}

/*
# 1 建表
hbase(main):001:0> create 'fruit1', 'info'
# 2 上传文件到HDFS
hdfs dfs -put /Users/zxy/IdeaProjects/bigdata-learning/hbase-learning/data/input/fruit.tsv /
# 3 执行命令，注意这里是hadoop里的yarn
/usr/local/hadoop-2.7.7/bin/yarn jar /Users/zxy/IdeaProjects/bigdata-learning/hbase-learning/target/hbase-learning-1.0-SNAPSHOT.jar com.zouxxyy.hbase.mr1.FruitDriver /fruit.tsv fruit1
 */