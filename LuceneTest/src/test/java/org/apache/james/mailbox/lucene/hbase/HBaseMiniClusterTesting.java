package org.apache.james.mailbox.lucene.hbase;

import java.io.IOException;

import org.junit.Test;

public class HBaseMiniClusterTesting {

    private static final HBaseClusterSingleton CLUSTER = HBaseClusterSingleton
            .build();

    // public static final byte[] table = Bytes.toBytes("index");
    // public static final byte[] tableCF = Bytes.toBytes("F");

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

    @Test
    public void insertDataIntoHBaseNodes() throws IOException {
        HBaseNames.TABLE.getName();
        CLUSTER.ensureTable(HBaseNames.TABLE.getName(),
                new byte[][] { HBaseNames.COLUMN_FAMILY.getName() });
    }
}
