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
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A write operation that can be included into a HAcidTxn.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidPut extends HAcidRowOperation {

    static Logger LOG = Logger.getLogger(HAcidPut.class.getName());

    private LinkedList<byte[]> families;
    private LinkedList<byte[]> qualifiers;
    private LinkedList<byte[]> values;
    private long _timestamp;
    private boolean executionHappened;
    private boolean commitHappened;

    public HAcidPut(HAcidTable table, byte[] row) throws IOException {

        LOG.debug("Initializing a HAcidPut on "+table.getTableNameString()+":"
            +Bytes.toString(row)+this.toString()
        );

        this.executionHappened = false;
        this.commitHappened = false;
        this._timestamp = Schema.TIMESTAMP_NULL_LONG;
        this.hacidtable = table;
        this.row = row;
        this.families = new LinkedList<byte[]>();
        this.qualifiers = new LinkedList<byte[]>();
        this.values = new LinkedList<byte[]>();
    }

    /**
     * Add the specified column and value to this HAcidPut operation.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public HAcidPut add(String family, String qualifier, String value) throws IOException {
        return add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    /**
     * Add the specified column and value to this HAcidPut operation.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public HAcidPut add(String family, String qualifier, byte[] value) throws IOException {
        return add(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
    }

    /**
     * Add the specified column and value to this HAcidPut operation.
     * 
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public HAcidPut add(byte[] family, byte[] qualifier, byte[] value) throws IOException {
        if(executionHappened) {
            LOG.warn("Cannot add to a HAcid put that has already been executed"
                +this.toString()
            );
            return this;
        }
        if(commitHappened) {
            LOG.warn("Cannot add to a HAcidPut that has already been committed"
                +this.toString()
            );
            return this;
        }

        LOG.debug("Setting "+Bytes.toString(family)+":"+Bytes.toString(qualifier)
            +" := "+Bytes.toString(value)+this.toString()
        );

        if( Bytes.equals(family,Schema.FAMILY_HACID) ) {
            LOG.warn("Can't do HAcidPut on a HAcid metadata column"
                +this.toString()
            );
            return this;
        }

        families.add(family);
        qualifiers.add(qualifier);
        values.add(value);

        return this;
    }

    public byte[] getRow() {
        return row;
    }

    @Override
    void execute(long timestamp) throws IOException {
        if(executionHappened) {
            LOG.warn("Cannot execute a HAcidPut twice."
                +this.toString()
            );
            return;
        }
        if(commitHappened) {
            LOG.warn("Cannot execute a HAcidPut after its commit."
                +this.toString()
            );
            return;
        }

        executionHappened = true;
        _timestamp = timestamp;

        LOG.debug("Executing the HAcidPut: modifying user table cells and "
            +"updating the committed-at metadata cell "+this.toString()
        );

        // add writeset user columns
        Put put = new Put(row, _timestamp);
        for(int i=0; i<families.size(); i++) {
            put.add(families.get(i), qualifiers.get(i), values.get(i));
        }

        // mark the user data as uncommitted
        put.add(
            Schema.FAMILY_HACID,
            Schema.QUALIFIER_USERTABLE_COMMITTED_AT,
            Schema.TIMESTAMP_NULL
        );

        hacidtable.put(put);
    }

    @Override
    void rollforward(long end_ts) throws IOException {
        if(!executionHappened) {
            LOG.warn("Cannot commit a HAcidPut before it is executed."
                +this.toString()
            );
            return;
        }
        if(commitHappened) {
            LOG.warn("Cannot commit a HAcidPut twice."
                +this.toString()
            );
            return;
        }

        commitHappened = true;

        LOG.debug("Committing a HAcidPut: writing the committed-at metadata"
            +" of the parent txn on the current user data"+this.toString()
        );

        Put usertable_put = new Put(row, _timestamp);
        usertable_put.add(
            Schema.FAMILY_HACID,
            Schema.QUALIFIER_USERTABLE_COMMITTED_AT,
            Bytes.toBytes(end_ts)
        );
        hacidtable.put(usertable_put);
    }

    @Override
    void rollback() throws IOException {
        if(!executionHappened) {
            LOG.warn("Cannot rollback a HAcidPut before it is executed."
                +this.toString()
            );
            return;
        }
        if(commitHappened) {
            LOG.warn("Cannot rollback a HAcidPut that is committed."
                +this.toString()
            );
            return;
        }

        LOG.debug("Rollbacking a HAcidPut: deleting all cells at the start "
            + "timestamp of the parent txn"+this.toString()
        );

        // delete, at this timestamp, all cells
        Delete d = new Delete(row);
        Get g = new Get(row);
        g.setTimeStamp(_timestamp);
        Result r = hacidtable.get(g);
        List<KeyValue> listKv = r.list();
        if(listKv != null && !listKv.isEmpty()) {
            for(KeyValue kv : listKv) {
                d.deleteColumn(kv.getFamily(), kv.getQualifier(), _timestamp);
            }
        }
        hacidtable.delete(d);
    }

    /**
     * Rebuilds part of the data of a HAcidPut given a writeset string of the
     * form "tableName:rowName".
     * 
     * @param writeEntry
     * @param conf
     * @return
     * @throws IOException
     */
    static HAcidPut restore(
        String writeEntry,
        Configuration conf,
        long txnStartTS) throws IOException 
    {
        String tablename = writeEntry.substring(0, writeEntry.indexOf(":"));
        String rowname   = writeEntry.substring(writeEntry.indexOf(":")+1);

        HAcidPut pput = new HAcidPut(
            new HAcidTable(conf, tablename),
            Bytes.toBytes(rowname)
        );

        pput._timestamp = txnStartTS;
        pput.executionHappened = true;
        pput.commitHappened = false;

        return pput;
    }

    @Override
    public String toString() {
        return " (put#"+super.hashCode()+")";
    }
}
