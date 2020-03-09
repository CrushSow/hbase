package com.aifeifeng.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(HDFSDriver.class);

        //关联 mapper 和reducer
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("fruit_hdfs",
                HDFSReducer.class,
                job);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        boolean result = job.waitForCompletion(true);
        return  result ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new HDFSDriver(), args);
        if (run==1){
            System.out.println("任务失败");
        }
    }
}
