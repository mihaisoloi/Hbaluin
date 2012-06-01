/**
 * 
 */
package org.apache.james.test;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author ms
 * 
 */
public class HBaseFile implements Serializable {

    private static final long serialVersionUID = 1l;

    protected ArrayList<byte[]> buffers = new ArrayList<byte[]>();
    long length;
    HBaseDirectory directory;
    protected long sizeInBytes;

    /**
     * This is publicly modifiable via Directory.touchFile(), so direct access
     * is not supported
     * 
     * @deprecated
     */
    @Deprecated
    private long lastModified = System.currentTimeMillis();

    /**
     * 
     */
    public HBaseFile(HBaseDirectory directory) {
        this.directory = directory;
    }

    /**
     * for buffering
     */
    public HBaseFile() {
        super();
    }

    /**
     * @return the lastModified
     */
    public long getLastModified() {
        return lastModified;
    }

    /**
     * @param lastModified
     *            the lastModified to set
     */
    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

}
