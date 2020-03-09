package com.aifeifeng.mr1;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class FruitMapper extends TableMapper<ImmutableBytesWritable,Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取put对象
        Put put = new Put(key.get());
        //获取该rowkey所有的cell
        Cell[] cells = value.rawCells();
        //遍历cell  找到所需的数据
        for (Cell cell : cells) {
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
                context.write(key,put);
            }
        }
    }
}
