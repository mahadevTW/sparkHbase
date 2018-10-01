package tephra_spike;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.hbase.TransactionAwareHTable;

import java.io.IOException;
import java.util.*;

public class SecondaryIndexTable {
    private byte[] secondaryIndex;
    private TransactionAwareHTable transactionAwareHTable;
    private TransactionAwareHTable secondaryIndexTable;
    private TransactionContext transactionContext;
    private final TableName secondaryIndexTableName;
    private static final byte[] secondaryIndexFamily =
            Bytes.toBytes("secondaryIndexFamily");
    private static final byte[] secondaryIndexQualifier = Bytes.toBytes('r');
    private static final byte[] DELIMITER  = new byte[] {0};

    public SecondaryIndexTable(TransactionServiceClient transactionServiceClient,
                               HTable hTable, byte[] secondaryIndex) {
        secondaryIndexTableName =
                TableName.valueOf(hTable.getName().getNameAsString() + ".idx");
        HTable secondaryIndexHTable = null;
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(hTable.getConfiguration());
            if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
                hBaseAdmin.createTable(new HTableDescriptor(secondaryIndexTableName));
            }
            secondaryIndexHTable = new HTable(hTable.getConfiguration(),
                    secondaryIndexTableName);
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                hBaseAdmin.close();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        this.secondaryIndex = secondaryIndex;
        this.transactionAwareHTable = new TransactionAwareHTable(hTable);
        this.secondaryIndexTable = new TransactionAwareHTable(secondaryIndexHTable);
        this.transactionContext = new TransactionContext(transactionServiceClient,
                transactionAwareHTable,
                secondaryIndexTable);
    }

    public Result get(Get get) throws IOException {
        return get(Collections.singletonList(get))[0];
    }

    public Result[] get(List<Get> gets) throws IOException {
        try {
            transactionContext.start();
            Result[] result = transactionAwareHTable.get(gets);
            transactionContext.finish();
            return result;
        } catch (Exception e) {
            try {
                transactionContext.abort();
            } catch (TransactionFailureException e1) {
                throw new IOException("Could not rollback transaction", e1);
            }
        }
        return null;
    }

    public Result[] getByIndex(byte[] value) throws IOException {
        try {
            transactionContext.start();
            Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
            scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier);
            ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

            ArrayList<Get> gets = new ArrayList<Get>();
            for (Result result : indexScanner) {
                for (Cell cell : result.listCells()) {
                    gets.add(new Get(cell.getValue()));
                }
            }
            Result[] results = transactionAwareHTable.get(gets);
            transactionContext.finish();
            return results;
        } catch (Exception e) {
            try {
                transactionContext.abort();
            } catch (TransactionFailureException e1) {
                throw new IOException("Could not rollback transaction", e1);
            }
        }
        return null;
    }

    public void put(Put put) throws IOException {
        put(Collections.singletonList(put));
    }


    public void put(List<Put> puts) throws IOException {
        try {
            transactionContext.start();
            ArrayList<Put> secondaryIndexPuts = new ArrayList<Put>();
            for (Put put : puts) {
                List<Put> indexPuts = new ArrayList<Put>();
                Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
                for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
                    for (KeyValue value : family.getValue()) {
                        if (value.getQualifier().equals(secondaryIndex)) {
                            byte[] secondaryRow = Bytes.add(value.getQualifier(),
                                    DELIMITER,
                                    Bytes.add(value.getValue(),
                                            DELIMITER,
                                            value.getRow()));
                            Put indexPut = new Put(secondaryRow);
                            indexPut.add(secondaryIndexFamily, secondaryIndexQualifier, put.getRow());
                            indexPuts.add(indexPut);
                        }
                    }
                }
                secondaryIndexPuts.addAll(indexPuts);
            }
            transactionAwareHTable.put(puts);
            secondaryIndexTable.put(secondaryIndexPuts);
            transactionContext.finish();
        } catch (Exception e) {
            try {
                transactionContext.abort();
            } catch (TransactionFailureException e1) {
                throw new IOException("Could not rollback transaction", e1);
            }
        }
    }
}

