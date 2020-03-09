package Uranus.Ares.Weibo;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * PACKAGE_NAMW   Uranus.Ares.Weibo
 * DATE      03
 * Author     Crush
 */
public class BusinessConnnectTest {
    HBaseConfiguration configuration= (HBaseConfiguration) HBaseConfiguration.create();
    public void publicContent(String uid,String content){
        HConnection connection=null;

        try {
            connection= HConnectionManager.getConnection(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
