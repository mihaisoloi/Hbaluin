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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.putBigDecimal;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseIndexOutput extends IndexOutput {

    private HBaseFile file;
    private Put put;
    private static HTable hTable;

    public HBaseIndexOutput(HTable hTable, Put put) {
        this.put=put;
        this.hTable=hTable;
    }

    /**
     * Forces any buffered output to be written.
     */
    @Override
    public void flush() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Closes this stream to further operations.
     */
    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Returns the current position in this file, where the next write will
     * occur.
     *
     * @see #seek(long)
     */
    @Override
    public long getFilePointer() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Sets current position in this file, where the next write will occur.
     *
     * @see #getFilePointer()
     */
    @Override
    public void seek(long pos) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * The number of bytes in the file.
     */
    @Override
    public long length() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Writes a single byte.
     *
     * @see org.apache.lucene.store.IndexInput#readByte()
     */
    @Override
    public void writeByte(byte b) throws IOException {
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     * @see org.apache.lucene.store.DataInput#readBytes(byte[], int, int)
     */
    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        put.add(Bytes.toBytes("f"),Bytes.toBytes("avro"),b);
        hTable.put(put);
    }
}
