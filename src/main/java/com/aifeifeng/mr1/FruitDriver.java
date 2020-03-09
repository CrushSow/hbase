package com.aifeifeng.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class FruitDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job对象
        Configuration configuration = HBaseConfiguration.create();

        Job job = Job.getInstance(configuration);

        //2 指定jar的所在路径
        job.setJarByClass(FruitDriver.class);

        //3 指定map和reduce
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("fruit",
                scan,
                FruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,job);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                FruitReducer.class,
                job);
        //4 指定 mapper的输出

        //5 指定最终输出

        //6 指定输入和输出的路径

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
