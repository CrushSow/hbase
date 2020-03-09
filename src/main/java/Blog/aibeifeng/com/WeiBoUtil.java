package Blog.aibeifeng.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class WeiBoUtil {
    /**
     * 创建命名空间
     */
    //获取hbase的配置信息
       static Configuration configuration = HBaseConfiguration.create();
       static {
           configuration.set("hbase.zookeeper.quorum","192.168.126.110");
       }
    public static void createNamespace(String ns) throws IOException {

        //获取hbase管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        //构建命名空间的描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        //创建namespace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }
    /**
     * 创建表
     */
    public static void createTable(String tablename,int version,String... cfs) throws IOException {
        //获取hbase管理员对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));

        for (String cf:cfs){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(version);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }
    //发布微博内容  和更新表的内容也会去添加
    public static void putData(String tableName,String uid,String cf,String cn,String
                              value ) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //封装put
        long ts = System.currentTimeMillis();
        String  rowkey=uid+"_"+ts;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //执行操作
        table.put(put);

        //更新收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table relationTable = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        Result result = relationTable.get(get);


        ArrayList<Put> puts = new ArrayList<Put>();
        for (Cell cell : result.rawCells()) {
            if ("fans".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){

                byte[] inboxRowkey = CellUtil.cloneQualifier(cell);

                Put inboxPut=new Put(inboxRowkey);
                inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),ts,
                        Bytes.toBytes(rowkey));
                puts.add(inboxPut);
            }
        }

        inboxTable.put(puts);

        inboxTable.close();
        table.close();
        connection.close();
    }
    /**
     * 添加关注用户（多个）
     *
     * 1 在用户关系表中  给当前用户添加  attends
     * 2 在用户关系表中  给被关注用户添加 fans
     * 3 在收件箱表中 给当前用户添加关注用户最近所发微博的rowkey
     *
     */
    public static void addAttends(String uid,String... attends) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));

        Put attendput = new Put(Bytes.toBytes(uid));
        ArrayList<Put> puts = new ArrayList<Put>();
        puts.add(attendput);
        for (String attend : attends) {
            attendput.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend),Bytes.toBytes(""));
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid),Bytes.toBytes(""));
            puts.add(put);
        }
        table.put(puts);

        //从内容表中取出来内容存放到收件箱表
        Table indoxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Contants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));

        if (attends.length<=0){
            return;
        }
        for (String attend: attends) {
            //startRow 和  stopRow
//            Scan scan = new Scan(Bytes.toBytes(attend), Bytes.toBytes(attend + "|"));

            //过滤器去  拿到数据
            Scan  scan=new Scan();
            //过滤器里面有很多过滤的 规则
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);

            //获取所有符合扫描规则的数据
            ResultScanner scanner = contentTable.getScanner(scan);

            if (scanner==null){
                return;
            }

            //循环遍历去除每条数据的rowkey 添加到inboxPut 中
            for (Result result : scanner) {
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attend),row);
                //往收件箱中给操作者添加数据
                indoxTable.put(inboxPut);
            }
        }




        indoxTable.close();
        contentTable.close();
        table.close();
        connection.close();

    }
    //移除（取关） 用户
    /**
     * 取关
     * 1 给用户关系表党总  删除当前用户的deletes
     * 2 在用户关系表中  删除被取关用户的额fans（操作者）
     * 3 在收件箱表中删除取关用户的所有的数据
     */

    public static void deleteRelation(String uid,String... deletes) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relationTable = connection.getTable(TableName.valueOf(Contants.RELATION_TABLE));
        ArrayList<Delete> deleteArrayList = new ArrayList<Delete>();
       // 1 给用户关系表党总  删除当前用户的deletes
        Delete userDelete = new Delete(Bytes.toBytes(uid));
        for (String delete : deletes) {
            userDelete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes("delete"));

           // 2 在用户关系表中  删除被取关用户的额fans（操作者）
            Delete fanDelete = new Delete(Bytes.toBytes(delete));
            fanDelete.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid));
            deleteArrayList.add(fanDelete);
        }
        deleteArrayList.add(userDelete);

        relationTable.delete(deleteArrayList);

       // 3 在收件箱表中删除取关用户的所有的数据
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));

        Delete inboxDelete= new Delete(Bytes.toBytes(uid));
        for (String delete:deletes){
            inboxDelete.addColumn(Bytes.toBytes("info"),Bytes.toBytes(delete));
        }
        inboxTable.delete(inboxDelete);

        relationTable.close();
        inboxTable.close();
        connection.close();

    }

    /**
     * 获取关注的人的微博内容
     */

    public static void getWeibo(String uid) throws IOException {
        //获取两个表的对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table inboxTable = connection.getTable(TableName.valueOf(Contants.INBOX_TABLE));
        Table contenetTable = connection.getTable(TableName.valueOf(Contants.CONTENT_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(3);//设置获取的参数

        Result result = inboxTable.get(get);
        for (Cell cell:result.rawCells()){
            byte[] contentRowkey = CellUtil.cloneValue(cell);
            Get contentGet = new Get(contentRowkey);
            Result contentResult = contenetTable.get(contentGet);
            for (Cell cell1:contentResult.rawCells()){
                String uid_ts = Bytes.toString(CellUtil.cloneRow(cell1));
                String id = uid_ts.split("_")[0];
                String ts = uid_ts.split("_")[1];


                String date = new SimpleDateFormat("yyyy--MM-dd HH:mm:ss").format(
                        new Date(Long.parseLong(ts))
                );

                System.out.println("用户" + id + ",时间" + date + ",内容：" + Bytes.toString(
                        CellUtil.cloneValue(cell1)
                ));
            }
        }

        inboxTable.close();
        contenetTable.close();
        connection.close();
    }

}














