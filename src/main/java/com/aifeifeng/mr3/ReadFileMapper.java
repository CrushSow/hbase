package com.aifeifeng.mr3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * PACKAGE_NAMW   com.aifeifeng.mr3
 * DATE      12
 * Author     Crush
 */
public class ReadFileMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] values = line.toString().split(",");

        String rowkey=values[0];


        byte[] bytes = Bytes.toBytes(rowkey);
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(values[1]));

        context.write(new ImmutableBytesWritable(bytes),put);
    }
}
