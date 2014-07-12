/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fi.aalto.hacid;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A transaction with read and write operations.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidTxn {

    static Logger LOG = Logger.getLogger(HAcidTxn.class.getName());

    private long start_ts;
    private long end_ts;
    private HAcidClient _client;
    private LinkedList<HAcidRowOperation> writes;
    private LinkedList<HAcidGet> reads;
    private boolean commitCalled;

    private HAcidTxn() {
    }

    public HAcidTxn(HAcidClient client) {
        LOG.debug("Initializing a new HAcidTxn"+this.toStringHash());
        init(client);
    }
    
    private void init(HAcidClient client) {
        start_ts = Schema.TIMESTAMP_NULL_LONG;
        end_ts = Schema.TIMESTAMP_NULL_LONG;
        commitCalled = false;
        _client = client;
        writes = new LinkedList<HAcidRowOperation>();
        reads = new LinkedList<HAcidGet>();
    }

    private void requestStartTimestampIfNecessary() throws IOException {
        if(start_ts == Schema.TIMESTAMP_NULL_LONG){
            LOG.debug("Get Start timestamp"+this.toStringHash());
            start_ts = _client.requestStartTimestamp();
        }
    }

    long getStartTimestamp() {
        if(start_ts == Schema.TIMESTAMP_NULL_LONG) {
            LOG.warn("The start timestamp of "+this.toStringHash()+" is null. Probably it shouldn't be.");
        }
        return start_ts;
    }

    long getEndTimestamp() {
        if(end_ts == Schema.TIMESTAMP_NULL_LONG) {
            LOG.warn("The end timestamp of txn "+this.toStringHash()+" is null. Probably it shouldn't be.");
        }
        return end_ts;
    }

    Collection<byte[]> getWriteset() {
        // Make column names for the col.family 'writeset'
        HashSet<byte[]> writesetRows = new HashSet<byte[]>(writes.size());
        for(HAcidRowOperation op : writes) {
            writesetRows.add(
                ArrayUtils.addAll(
                    ArrayUtils.addAll(
                        op.hacidtable.getTableName(),
                        Bytes.toBytes(":")
                    ),
                    op.row
                )
            );
        }
        return writesetRows;
    }

    Collection<byte[]> getReadset() {
        // Make column names for the col.family 'readset'
        HashSet<byte[]> readsetRows = new HashSet<byte[]>(reads.size());
        for(HAcidGet pg : reads) {
            readsetRows.add(
                ArrayUtils.addAll(
                    ArrayUtils.addAll(
                        pg.hacidtable.getTableName(),
                        Bytes.toBytes(":")
                    ),
                    pg.row
                )
            );
        }
        return readsetRows;
    }

    /**
     * Retrieves data at the Snapshot Isolated Read Timestamp for a given
     * HAcidGet (which specifies table, row, and possibly columns).
     *
     * @param get
     * @return
     * @throws IOException
     */
    public Result get(HAcidGet hacidget) throws IOException {
        LOG.debug("Executing a HAcidGet"+this.toStringHash());

        // This call was delayed until we really need it
        requestStartTimestampIfNecessary();

        // Remember this operation
        reads.add(hacidget);

        return _client.getFromReadTimestamp(hacidget, start_ts);
    }

    /**
     * Writes to the user table a new value in the specified cell. Does not yet
     * commit the data.
     *
     * @param usertable
     * @param row
     * @param family
     * @param qualifier
     * @param value
     * @throws Exception
     */
    public void put(HAcidPut hacidput) throws IOException {
        if(commitCalled) {
            LOG.warn("Can't HAcidPut more into a txn after it has been "
                + "committed"+this.toStringHash()
            );
            return;
        }

        LOG.debug("Preparing to execute HAcidPut "
            +hacidput.toString()+this.toStringHash()
        );

        // This call was delayed until we really need it
        requestStartTimestampIfNecessary();

        // Execute changes
        hacidput.execute(start_ts);

        // Remember this operation
        writes.add(hacidput);
    }

    /**
     * Writes to the user table a new value in the specified cell. Does not yet
     * commit the data.
     *
     * @param usertable
     * @param row
     * @param family
     * @param qualifier
     * @param value
     * @throws Exception
     */
    public void delete(HAcidDelete haciddelete) throws IOException {
        if(commitCalled) {
            LOG.warn("Can't HAcidDelete in a txn after it has been committed"
                +this.toStringHash()
            );
            return;
        }

        LOG.debug("Preparing to execute HAcidDelete "+haciddelete.toString()
            +this.toStringHash()
        );

        // Retrieves the start timestamp.
        // This call was delayed until we really need it
        requestStartTimestampIfNecessary();

        // Execute changes
        haciddelete.execute(start_ts);

        // Remember this operation
        writes.add(haciddelete);
    }

    /**
     * Requests HAcid to commit this transaction. Must be called to persist
     * the changes.
     *
     * @return true if the decision was commit, false otherwise.
     * @throws Exception
     */
    public boolean commit() throws IOException {
        return commit(HAcidClient.DEFAULT_CONFLICT_TEST_ATTEMPTS);
    }

    /**
     * Requests HAcid to commit this transaction. Must be called to persist
     * the changes.
     *
     * @param conflictTestAttempts Number of retries in transaction conflict testing.
     * @return true if the decision was commit, false otherwise.
     * @throws Exception
     */
    public boolean commit(int conflictTestAttempts) throws IOException {
        // No need to commit a transaction that did nothing
        if(start_ts == Schema.TIMESTAMP_NULL_LONG) {
            return true;
        }
        // No need to commit a read-only transaction
        if(writes.isEmpty()) {
            end_ts = start_ts; // read-only txn has end-ts == start-ts
            try {
                _client.setStateCommitted(this); // writes the state to HBase
            }
            catch (StateDisagreementException ex) {
                LOG.warn("State disagreement happened while setting read-only txn's state to committed. "+ex);
            }
            catch (IOException ex) {
                throw ex;
            }
            return true;
        }

        requestEndTimestamp(); // part 1 of commit
        return decideCommitOrAbort(conflictTestAttempts); // part 2 of commit
    }

    /**
     * The commit() procedure was separated into two parts to allow testing
     * of concurrent scenarios.
     *
     * In the first part, the end timestamp is obtained and it is registered
     * in the Timestamp Log along with information about the current transaction.
     *
     * @throws IOException
     */
    void requestEndTimestamp() throws IOException {
        commitCalled = true;

        LOG.debug("Get End timestamp for "+this.toStringHash());
        
        end_ts = _client.requestEndTimestamp(this);

        LOG.debug("Requested the commit for HAcidTxn "+this.toString()+this.toStringHash());
    }

    boolean decideCommitOrAbort() throws IOException {
        return decideCommitOrAbort(HAcidClient.DEFAULT_CONFLICT_TEST_ATTEMPTS);
    }

    /**
     * The commit() procedure was separated into two parts to allow testing
     * of concurrent scenarios.
     *
     * In the second part, previous transactions are scanned to check for conflicts,
     * and a commit/abort decision is taken.
     *
     * @param conflictCheckAttempts Number of retries in transaction conflict testing.
     * @return true if the decision was commit, false otherwise.
     * @throws IOException
     */
    boolean decideCommitOrAbort(int conflictCheckAttempts) throws IOException {
        byte[] state = null;
        try {
            if(_client.canCommit(this, conflictCheckAttempts)) {
                _client.setStateCommitted(this); // writes the state to HBase
                state = Schema.STATE_COMMITTED; // local variable
            }
            else {
                _client.setStateAborted(this);
                state = Schema.STATE_ABORTED;
            }
        }
        catch (StateDisagreementException ex) {
            LOG.warn("State disagreement happened while deciding commit/abort. "+ex);
            // Get the state (committed/aborted)
            state = _client.getTimestampData(start_ts)
                .getColumnLatest(Schema.FAMILY_HACID,Schema.QUALIFIER_TXN_STATE)
                .getValue()
            ;
        }
        catch (IOException ex) {
            throw ex;
        }
        finally {
            if(state==null || state==Schema.STATE_ACTIVE) {
                LOG.error("Control flow reached an unexpected/unpredicted "
                    + "position!"
                );
                return false;
            }
            else {
                if(Bytes.equals(state, Schema.STATE_COMMITTED)) {
                    // Update the committed timestamps in the user tables
                    rollforwardWrites();
                    writes.clear();
                    return true;
                }
                else {//if(Bytes.equals(state, Schema.STATE_ABORTED)) {
                    // Delete the data written to the writeset
                    rollbackWrites();
                    writes.clear();
                    return false;
                }
            }
        }
    }
    
    void rollforwardWrites() throws IOException {
        for(HAcidRowOperation op : writes) {
            op.rollforward(end_ts);
        }
    }

    void rollbackWrites() throws IOException{
        for(HAcidRowOperation op : writes) {
            op.rollback();
        }
    }

    /**
     * Rebuilds the HAcidTxn that generated the transaction commit data at
     * the row given by <code>rowkey</code> in the HAcid Timestamp Log.
     *
     * @param client
     * @param endTimestamp the end timestamp of the txn to be restored
     * @return
     */
    public static HAcidTxn restore(HAcidClient client, long endTimestamp) throws IOException {
        Result res = client.getTimestampData(endTimestamp);
        return restore(client, res);
    }

    /**
     * Rebuilds the HAcidTxn that generated the transaction commit data at the
     * row given by the Get result, but does not restore the writeset.
     *
     * @param client
     * @param rowkey
     * @return
     */
    static HAcidTxn lightRestore(HAcidClient client, Result endTimestampResult) throws IOException {
        boolean errors = true;
        try {
            if(! Bytes.equals(Schema.TYPE_END,
                endTimestampResult.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TS_TYPE
                ).getValue()
            )){
                throw new Exception("You must supply a end timestamp.");
            }
            errors = false;
        }
        catch(Exception ex) {
            LOG.error(ex);
        }
        finally {
            if(errors) {
                LOG.warn("To restore a transaction, you must supply its commit "
                    + "timestamp. Restoring a transaction with no end timestamp "
                    + "is not in the contract of this method."
                );
                return null;
            }
        }

        HAcidTxn restoredTxn = new HAcidTxn();
        restoredTxn.init(client);

        // Restore start timestamp
        restoredTxn.start_ts = Bytes.toLong(
            endTimestampResult.getColumnLatest(
                Schema.FAMILY_HACID,
                Schema.QUALIFIER_START_TIMESTAMP
            )
            .getValue()
        );

        // Restore end timestamp
        restoredTxn.end_ts = HAcidClient.keyToTimestamp(endTimestampResult.getRow());

        // Restore commitCalled
        restoredTxn.commitCalled = true; // always true because we already have an end timestamp
        
        return restoredTxn;
    }

    /**
     * Rebuilds the HAcidTxn that generated the transaction commit data at the
     * row given by the Get result.
     *
     * @param client
     * @param rowkey
     * @return
     */
    static HAcidTxn restore(HAcidClient client, Result endTimestampResult) throws IOException {

        HAcidTxn restoredTxn = lightRestore(client, endTimestampResult);

        // Restore writes
        NavigableMap<byte[],byte[]> writeset =
            endTimestampResult.getFamilyMap(
                Schema.FAMILY_WRITESET
            )
        ;
        Entry<byte[],byte[]> writeEntry;
        while((writeEntry = writeset.pollFirstEntry()) != null) {
            HAcidPut pput = HAcidPut.restore(
                Bytes.toString(writeEntry.getKey()),
                client.configuration_HBase,
                restoredTxn.start_ts
            );
            restoredTxn.writes.add(pput);
        }

        // Restore reads
        NavigableMap<byte[],byte[]> readset =
            endTimestampResult.getFamilyMap(
                Schema.FAMILY_READSET
            )
        ;
        Entry<byte[],byte[]> readEntry;
        while((readEntry = readset.pollFirstEntry()) != null) {
            HAcidGet pget = HAcidGet.restore(
                Bytes.toString(readEntry.getKey()),
                client.configuration_HBase
            );
            restoredTxn.reads.add(pget);
        }

        return restoredTxn;
    }

    @Override
    public String toString() {
        if(start_ts == Schema.TIMESTAMP_NULL_LONG) {
            return "unidentifiable_txn";
        }
        else if(end_ts == Schema.TIMESTAMP_NULL_LONG) {
            return ("(start_ts="+start_ts+", end_ts=[uncommitted])");
        }
        return ("(start_ts="+start_ts+", end_ts="+end_ts+")");
    }

    public String toStringHash() {
        return " ("+"txn#"+super.hashCode()+")";
    }
}
