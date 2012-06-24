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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;

public class HBaseFile implements Serializable {

    private static final long serialVersionUID = 1l;
    protected ArrayList<byte[]> buffers = new ArrayList<byte[]>();
    public HBaseDirectory directory;
    protected long sizeInBytes;

    // This is publicly modifiable via Directory.touchFile(), so direct access not supported
    private long lastModified = System.currentTimeMillis();

    // For non-stream access from thread that might be concurrent with writing
    public synchronized long getLength() {
        return buffers.size();
    }

    // For non-stream access from thread that might be concurrent with writing
    public synchronized long getLastModified() {
        return lastModified;
    }

    protected synchronized void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    protected final byte[] addBuffer(int size) {
        byte[] buffer = newBuffer(size);
        synchronized (this) {
            buffers.add(buffer);
            sizeInBytes += size;
        }

        if (directory != null) {
//            directory.sizeInBytes.getAndAdd(size);
        }
        return buffer;
    }

    protected final synchronized byte[] getBuffer(int index) {
        return buffers.get(index);
    }

    protected final synchronized int numBuffers() {
        return buffers.size();
    }

    /**
     * Expert: allocate a new buffer.
     * Subclasses can allocate differently.
     *
     * @param size size of allocated buffer.
     * @return allocated buffer.
     */
    protected byte[] newBuffer(int size) {
        return new byte[size];
    }

    public synchronized long getSizeInBytes() {
        return sizeInBytes;
    }

}
