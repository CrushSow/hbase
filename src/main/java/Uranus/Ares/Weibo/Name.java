package Uranus.Ares.Weibo;

import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * PACKAGE_NAMW   Uranus.Ares.Weibo
 * DATE      03
 * Author     Crush
 */
public class Name {
    static HBaseConfiguration configuration= (HBaseConfiguration) HBaseConfiguration.create();
    private static final byte[]  TABLE_NAME= Bytes.toBytes("weibo:content");
    private static final byte[]  TABLE_RELATION= Bytes.toBytes("weibo:relation");
    private static final byte[]  TABLE_RECEIVER_COMMENT_EMAIL= Bytes.toBytes("weibo:receiver_comment_email");

    public void intial(){
        HBaseAdmin admin=null;

        try {
            admin=new HBaseAdmin(configuration);

            NamespaceDescriptor weibo=NamespaceDescriptor.create("weibo").
                    addConfiguration("creator","crush").
                    addConfiguration("create_time",System.currentTimeMillis()+"").build();
            admin.createNamespace(weibo);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void createTableContent(){
        HBaseAdmin admin=null;

        try {
            admin=new HBaseAdmin(configuration);
            HTableDescriptor content= new HTableDescriptor(TableName.valueOf("Table_CONTENE"));
            HColumnDescriptor info =new HColumnDescriptor(Bytes.toBytes("info"));

            info.setBlockCacheEnabled(true);
            info.setBlocksize(2097152);
            info.setCompressionType(Compression.Algorithm.SNAPPY);
            info.setMaxVersions(1);
            content.addFamily(info);
            admin.createTable(content);
        } catch (IOException e) {
            System.out.println(e.getStackTrace());
        }finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
