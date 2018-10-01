package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertDataHbase {
    public static void main(String[] args) throws IOException {
        Configuration config = getHbaseConf();
        HTable hTable = new HTable(config, "emp");
        Put put = new Put(Bytes.toBytes("row11"));
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Maha"));
        hTable.put(put);
        hTable.close();
    }

    public static Configuration getHbaseConf() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "127.0.0.1:60000");
        config.addResource("/usr/local/opt/hbase/libexec/conf/hbase-site.xml");
        config.setInt("timeout", 120000);
        config.set("hbase.zookeeper.quorum", "127.0.0.1");
        config.setInt("hbase.zookeeper.property.clientPort", 2181);
        config.set("zookeeper.znode.parent", "/hbase");
        config.setInt("hbase.client.scanner.caching", 10000);
        return config;
    }
}