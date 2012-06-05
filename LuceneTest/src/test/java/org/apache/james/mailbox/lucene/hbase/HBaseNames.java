package org.apache.james.mailbox.lucene.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public enum HBaseNames {
    TABLE("index"), COLUMN_FAMILY("F");

    public final byte[] name;

    private HBaseNames(String name) {
        this.name = toBytes(name);
    }

}