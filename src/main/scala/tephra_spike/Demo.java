package tephra_spike;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.distributed.SingleUseClientProvider;
import org.apache.tephra.distributed.ThriftClientProvider;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.tephra.runtime.*;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws IOException {
        Connection connection = ConnectionFactory.createConnection();
//        Admin admin = connection.getAdmin();
        TableName table1 = TableName.valueOf("emp");
//        String family1 = "Family1";
//        String family2 = "Family2";
//        HTableDescriptor desc = new HTableDescriptor(table1);
//        desc.addFamily(new HColumnDescriptor(family1));
//        desc.addFamily(new HColumnDescriptor(family2));
//        admin.createTable(desc);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "127.0.0.1:60000");
        config.addResource("/usr/local/opt/hbase/libexec/conf/hbase-site.xml");
        config.setInt("timeout", 120000);
        config.set("hbase.zookeeper.quorum", "localhost");
        config.setInt("hbase.zookeeper.property.clientPort", 2181);
        config.set("zookeeper.znode.parent", "/hbase");
        config.setInt("hbase.client.scanner.caching", 10000);
        TransactionServiceClient txClient;

        Injector injector = Guice.createInjector(
                new ConfigModule(config),
                new ZKModule(),
                new DiscoveryModules().getDistributedModules(),
                new TransactionModules().getDistributedModules(),
                new TransactionClientModule()
        );

        txClient = injector.getInstance(TransactionServiceClient.class);
        HTable hTable = new HTable(table1, connection);
        TransactionAwareHTable t = new TransactionAwareHTable(hTable);
        TransactionContext context = new TransactionContext(txClient, t);
        SecondaryIndexTable s = new SecondaryIndexTable(txClient, hTable, "hi".getBytes());
        Put put1 = new Put(Bytes.toBytes("row5"));
//        HTable htable = connection.getTable(TableName.valueOf("emp"))
        put1.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("On Car"));
        try {
            context.start();
            t.put(put1);
            context.finish();
        } catch (TransactionFailureException e) {
            e.printStackTrace();
        }
    }
}
