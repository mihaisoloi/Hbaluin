package org.apache.james.mailbox.lucene.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.COLUMN_FAMILY;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.avro.AvroUtil;
import org.apache.hadoop.hbase.avro.generated.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMiniClusterTesting {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseMiniClusterTesting.class);
    private static final HBaseClusterSingleton CLUSTER = HBaseClusterSingleton
            .build();
    private static HBaseAdmin admin = null;

    @Before
    public void setUp() throws IOException {
        CLUSTER.ensureTable(TABLE.name, new byte[][] { COLUMN_FAMILY.name });
        admin = new HBaseAdmin(CLUSTER.getConf());
    }

    @After
    public void tearDown() {
        IOUtils.closeStream(admin);
    }

    @Test
    public void insertDataIntoHBaseNodes() throws IOException {
        assertTrue(admin.tableExists(TABLE.name));
        HTableDescriptor htd = admin.getTableDescriptor(TABLE.name);
        assertTrue(htd.getFamiliesKeys().contains(COLUMN_FAMILY.name));
    }

    @Test
    public void insertValuesIntoColumns() throws IOException {
        HTable htable = new HTable(CLUSTER.getConf(), TABLE.name);
        Put put = new Put(toBytes("mihai"));
        put.add(COLUMN_FAMILY.name, toBytes("varsta"), toBytes(27));
        put.add(COLUMN_FAMILY.name, toBytes("sex"), toBytes("male"));
        htable.put(put);
        htable.flushCommits();
        Get get = new Get(toBytes("mihai"));
        get.addFamily(COLUMN_FAMILY.name);
        Result result = htable.get(get);
        NavigableMap<byte[], byte[]> myMap = result
                .getFamilyMap(COLUMN_FAMILY.name);
        assertEquals(27, Bytes.toInt(myMap.get(toBytes("varsta"))));
        assertEquals("male", Bytes.toString(myMap.get(toBytes("sex"))));
        htable.close();
    }

    @Test
    public void testAvroHBaseIntegration() throws IOException {
        ClusterStatus cs = admin.getClusterStatus();
        assertEquals(1, cs.getServersSize());
        AClusterStatus acs = AvroUtil.csToACS(cs);
        assertEquals(cs.getServersSize(), acs.servers);
    }

}
