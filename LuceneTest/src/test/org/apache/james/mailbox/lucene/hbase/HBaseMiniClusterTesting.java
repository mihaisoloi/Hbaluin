package org.apache.james.mailbox.lucene.hbase;

import org.junit.Test;

public class HBaseMiniClusterTesting {

    private static final HBaseClusterSingleton CLUSTER = HBaseClusterSingleton
            .build();

    // public static final byte[] table = Bytes.toBytes("index");
    // public static final byte[] tableCF = Bytes.toBytes("F");

    public enum HBaseNames {
        TABLE("index"), COLUMN_FAMILY("F");

        private final String name;

        HBaseNames(String name) {
            this.name = name;
        }

        public byte[] getName() {
            return name.getBytes();
        }
    }

    @Test
    public void insertDataIntoHBaseNodes() {
        HBaseNames.TABLE.getName();

    }
}
