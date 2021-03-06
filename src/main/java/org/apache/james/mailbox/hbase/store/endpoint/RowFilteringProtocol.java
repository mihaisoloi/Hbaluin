package org.apache.james.mailbox.hbase.store.endpoint;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.james.mailbox.hbase.store.MessageFields;

import java.io.IOException;
import java.util.Set;

public interface RowFilteringProtocol extends CoprocessorProtocol{

    public Set<Long> filterByQueries(byte[] mailboxId, Multimap<MessageFields, String> queries) throws IOException;

    public Set<Long> filterByMailbox(byte[] mailboxId) throws IOException;
}
