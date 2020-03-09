package com.aifeifeng.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDFSMapper extends Mapper<LongWritable,Text,NullWritable,Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001  apple   red

        String[] split = value.toString().split("\t");

        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),
                Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("color"),
                Bytes.toBytes(split[2]));
        context.write(NullWritable.get(),put);
    }
}
