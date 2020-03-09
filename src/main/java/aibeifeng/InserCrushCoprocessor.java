package aibeifeng;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * PACKAGE_NAMW   aibeifeng
 * DATE      10
 * Author     Crush
 *
 * 协处理器（hbase自己的功能）
 * 创建类  继承BaseregionObserver
 * 重写方法 postPut
 * 实现逻辑  将增加到student的数据加到 crush:student中去
 *
 *将项目打包（依赖）后上传到hbase中去，让hbase可以是被协处理器
 */
public class InserCrushCoprocessor extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表格
        HTableInterface table = e.getEnvironment().getTable(TableName.valueOf("crush:Student"));

        table.put(put);
        table.close();
    }
}
