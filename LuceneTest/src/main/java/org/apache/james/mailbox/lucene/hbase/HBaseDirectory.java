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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ThreadInterruptedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.james.mailbox.lucene.hbase.HBaseNames.*;

public class HBaseDirectory extends Directory implements Serializable {


//    private static final Log LOG = LogFactory.getLog(HBaseDirectory.class);

    //keys are the segment_name and lucene inverted index(i.e. contents of the segment file)
    public static final Map<String, HBaseFile> hBaseFileMap = new ConcurrentHashMap<String, HBaseFile>();

    protected final AtomicLong sizeInBytes = new AtomicLong();
    private Configuration config;

    /**
     * Constructs an empty {@link Directory}.
     */
    public HBaseDirectory() {
        try {
            //temporary single instance LockFactory
            setLockFactory(new SingleInstanceLockFactory());
        } catch (IOException e) {
            // Cannot happen
        }
    }

    public HBaseDirectory(Configuration config) {
        this();
        this.config = config;
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(config);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }

        try {
            if (!admin.tableExists(SEGMENTS.name)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(SEGMENTS.name);
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(TERM_DOCUMENT_CF.name);
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return hBaseFileMap.keySet().toArray(new String[0]);
    }

    /**
     * looking for a certain segment file
     *
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public boolean fileExists(String name) throws IOException {
        ensureOpen();
        return hBaseFileMap.containsKey(name);
    }

    /**
     * Returns the time the named file was last modified.
     *
     * @throws IOException if the file does not exist
     */
    @Override
    @Deprecated
    public long fileModified(String name) throws IOException {
        ensureOpen();
        HBaseFile hBaseFile = hBaseFileMap.get(name);
        if (hBaseFile == null) {
            throw new FileNotFoundException(name);
        }
        return hBaseFile.getLastModified();
    }

    /**
     * Set the modified time of an existing file to now.
     *
     * @throws IOException if the file does not exist
     * @deprecated Lucene never uses this API; it will be
     *             removed in 4.0.
     */
    @Override
    @Deprecated
    public void touchFile(String name) throws IOException {
        ensureOpen();
        HBaseFile file = hBaseFileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }

        long ts2, ts1 = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException ie) {
                throw new ThreadInterruptedException(ie);
            }
            ts2 = System.currentTimeMillis();
        } while (ts1 == ts2);

        file.setLastModified(ts2);
    }

    /**
     * Removes an existing file in the directory.
     *
     * @throws IOException if the file does not exist
     */
    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        HBaseFile hBaseFile = hBaseFileMap.remove(name);
        if (hBaseFile != null) {
            hBaseFile.directory = null;
            sizeInBytes.addAndGet(hBaseFile.sizeInBytes);
        } else {
            throw new FileNotFoundException(name);
        }
    }

    /**
     * Returns the length in bytes of a file in the directory.
     *
     * @throws IOException if the file does not exist
     */
    @Override
    public final long fileLength(String name) throws IOException {
        ensureOpen();
        HBaseFile file = hBaseFileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return file.getLength();
    }

    /**
     * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
     */
    @Override
    public IndexOutput createOutput(String name) throws IOException {
        ensureOpen();
        HBaseFile hBaseFile = new HBaseFile();

        hBaseFileMap.put(name, hBaseFile);
        return new HBaseIndexOutput(name);
    }

    protected class HBaseIndexOutput extends BufferedIndexOutput {

        private final HBaseFile file;
        private final String name;

        public HBaseIndexOutput(String name) {
            this.file = hBaseFileMap.get(name);
            this.name = name;
        }

        /**
         * Expert: implements buffer write.  Writes bytes at the current position in
         * the output.
         *
         * @param b      the bytes to write
         * @param offset the offset in the byte array
         * @param len    the number of bytes to write
         */
        @Override
        protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
            Put put = new Put(toBytes(name));
            HTable hTable = null;
            try {
                hTable = new HTable(config, SEGMENTS.name);
                byte[] write = Arrays.copyOfRange(b, offset, offset + len);

                //adding the contents of the file to memory as well
                file.buffers.add(write);

                put.add(TERM_DOCUMENT_CF.name, AVRO_QUALIFIER.name, write);
                hTable.put(put);
            } finally {
                hTable.close();
            }
        }

        /**
         * The number of bytes in the file.
         */
        @Override
        public long length() throws IOException {
            return file.getLength();
        }
    }

    /**
     * Returns a stream reading an existing file.
     */
    @Override
    public IndexInput openInput(String name) throws IOException {
        ensureOpen();
        return new HBaseIndexInput(name,config);
    }

    /**
     * Closes the store to future operations, releasing associated memory.
     */
    @Override
    public void close() {
        isOpen = false;
        hBaseFileMap.clear();
    }
}
