/******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more         *
 * contributor license agreements. See the NOTICE file distributed with       *
 * this work for additional information regarding copyright ownership.        *
 * The ASF licenses this file to You under the Apache License, Version 2.0    *
 * (the "License"); you may not use this file except in compliance with       *
 * the License. You may obtain a copy of the License at                       *
 *                                                                            *
 * http://www.apache.org/licenses/LICENSE-2.0                                 *
 *                                                                            *
 * Unless required by applicable law or agreed to in writing, software        *
 * distributed under the License is distributed on an "AS IS" BASIS,          *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 * See the License for the specific language governing permissions and        *
 * limitations under the License.                                             *
 ******************************************************************************/

package org.apache.james.mailbox.lucene.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.lucene.store.IndexInput;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.NavigableMap;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseIndexInput extends IndexInput {

    private final String name;
    private final Configuration config;


    public HBaseIndexInput(String name,Configuration config) {
        super("HBaseIndexInput(name=" + name + ")");
        this.name=name;
        this.config=config;
    }

    /**
     * Closes the stream to further operations.
     */
    @Override
    public void close() throws IOException {
        // nothing to do here
    }

    /**
     * Returns the current position in this file, where the next read will
     * occur.
     *
     * @see #seek(long)
     */
    @Override
    public long getFilePointer() {
        return 0;
    }

    /**
     * Sets current position in this file, where the next read will occur.
     *
     * @see #getFilePointer()
     */
    @Override
    public void seek(long pos) throws IOException {
    }

    /**
     * The number of bytes in the file.
     */
    @Override
    public long length() {
        Get get = new Get(toBytes(name));
        get.addColumn(TERM_DOCUMENT_CF.name, AVRO_QUALIFIER.name);
        HTable hTable = null;
        try {
            hTable = new HTable(config, SEGMENTS.name);
            Result result = hTable.get(get);
            NavigableMap<byte[], byte[]> myMap = result.getFamilyMap(TERM_DOCUMENT_CF.name);
            if (myMap != null) { //index does not exist
                byte[] value = myMap.get(AVRO_QUALIFIER.name);
                return value.length;
            }
        } catch (IOException ioe) {
            System.err.print("Exception while computing the size of the bytes to be read from the HTable!");
        } finally {
            try {
                hTable.close();
            } catch (IOException ioe) {
                //do nothing
            }
        }
        return 0;
    }

    /**
     * Reads and returns a single byte.
     *
     * @see org.apache.lucene.store.DataOutput#writeByte(byte)
     */
    @Override
    public byte readByte() throws IOException {
        byte[] bytes = new byte[1];
        readBytes(bytes,0,1);
        return bytes[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     * @see org.apache.lucene.store.DataOutput#writeBytes(byte[], int)
     */
    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        Get get = new Get(toBytes(name));
        get.addColumn(TERM_DOCUMENT_CF.name, AVRO_QUALIFIER.name);
        HTable hTable = null;
        try {
            hTable = new HTable(config, SEGMENTS.name);
            Result result = hTable.get(get);
            NavigableMap<byte[], byte[]> myMap = result.getFamilyMap(TERM_DOCUMENT_CF.name);
            if (myMap != null) { //index does not exist
                byte[] value = myMap.get(AVRO_QUALIFIER.name);
                b = value;
            }
        } catch (IOException ioe) {
            System.err.print("Exception while computing the size of the bytes to be read from the HTable!");
        } finally {
            try {
                hTable.close();
            } catch (IOException ioe) {
                //do nothing
            }
        }
    }
}
