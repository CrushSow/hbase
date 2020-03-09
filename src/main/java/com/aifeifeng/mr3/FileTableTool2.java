package com.aifeifeng.mr3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * PACKAGE_NAMW   com.aifeifeng.mr3
 * DATE      12
 * Author     Crush
 */
public class FileTableTool2 implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        Job job= Job.getInstance();
        job.setJarByClass(FileTableTool2.class);

        // format
        Path path=new Path("hdfs://hadoop:8020/data/student.csv");
        FileInputFormat.addInputPath(job,path);

        // Map
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // reducer



        return  job.waitForCompletion(true)? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();



    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
