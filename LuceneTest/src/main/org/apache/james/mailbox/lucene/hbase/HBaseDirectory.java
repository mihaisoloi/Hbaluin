package org.apache.james.mailbox.lucene.hbase;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 *
 */
public class HBaseDirectory extends Directory {

    /**
     * 
     */
    public HBaseDirectory() {
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#listAll()
     */
    @Override
    public String[] listAll() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#fileExists(java.lang.String)
     */
    @Override
    public boolean fileExists(String name) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public long fileModified(String name) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public void touchFile(String name) throws IOException {
        ensureOpen();
        HBaseFile file = new HBaseFile();// TODO: somehow find the file in HBase
        if (file == null) {
            throw new FileNotFoundException(name);
        }

        file.setLastModified(System.currentTimeMillis());
    }

    /*
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#fileLength(java.lang.String)
     */
    @Override
    public long fileLength(String name) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#createOutput(java.lang.String)
     */
    @Override
    public IndexOutput createOutput(String name) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#openInput(java.lang.String)
     */
    @Override
    public IndexInput openInput(String name) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.lucene.store.Directory#close()
     */
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

}
