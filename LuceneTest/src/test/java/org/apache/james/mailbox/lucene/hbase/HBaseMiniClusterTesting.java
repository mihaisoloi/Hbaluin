package org.apache.james.mailbox.lucene.hbase;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HBaseMiniClusterTesting {

    private static final HBaseClusterSingleton CLUSTER = HBaseClusterSingleton
            .build();
    private static HBaseAdmin admin = null;

    @Before
    public void setUp() throws IOException {
        CLUSTER.ensureTable(HBaseNames.TABLE.getName(),
                new byte[][] { HBaseNames.COLUMN_FAMILY.getName() });
        admin = new HBaseAdmin(CLUSTER.getConf());
    }

    @After
    public void after() {
        IOUtils.closeStream(admin);
    }

    @Test
    public void insertDataIntoHBaseNodes() throws IOException {
        Assert.assertTrue(admin.tableExists(HBaseNames.TABLE.getName()));
        HTableDescriptor htd = admin.getTableDescriptor(HBaseNames.TABLE
                .getName());
        Assert.assertTrue(htd.getFamiliesKeys().contains(
                HBaseNames.COLUMN_FAMILY.getName()));
    }

    @Test
    public void insertValuesIntoColumns() throws IOException {
        HTable htable = null;
        htable = new HTable(CLUSTER.getConf(), HBaseNames.TABLE.getName());
        Put put = new Put(Bytes.toBytes("mihai"));
        put.add(HBaseNames.COLUMN_FAMILY.getName(), Bytes.toBytes("varsta"),
                Bytes.toBytes(26));
        put.add(HBaseNames.COLUMN_FAMILY.getName(), Bytes.toBytes("sex"),
                Bytes.toBytes("male"));
        htable.put(put);
        htable.flushCommits();
        Get get = new Get(Bytes.toBytes("mihai"));
        get.addFamily(HBaseNames.COLUMN_FAMILY.getName());
        Result result = htable.get(get);
        NavigableMap<byte[], byte[]> myMap = result
                .getFamilyMap(HBaseNames.COLUMN_FAMILY.getName());
        Assert.assertEquals(26, Bytes.toInt(myMap.get(Bytes.toBytes("varsta"))));
        Assert.assertEquals("male",
                Bytes.toString(myMap.get(Bytes.toBytes("sex"))));
        htable.close();
    }

    public enum HBaseNames {
        TABLE("index"), COLUMN_FAMILY("F");

        public final String name;

        HBaseNames(String name) {
            this.name = name;
        }

        public byte[] getName() {
            return name.getBytes();
        }
    }
}
