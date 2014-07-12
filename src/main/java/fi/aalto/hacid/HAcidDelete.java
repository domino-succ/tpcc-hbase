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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A delete operation that can be included into a HAcidTxn.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidDelete extends HAcidRowOperation {

    static Logger LOG = Logger.getLogger(HAcidPut.class.getName());

    private HAcidPut _hacidput;
    private boolean hasSpecifiedColumns;
    private boolean executionHappened;

    public HAcidDelete(HAcidTable table, byte[] row) throws IOException {
        this.hacidtable = table;
        this.row = row;
        this._hacidput = new HAcidPut(table, row);
        this.hasSpecifiedColumns = false;
        this.executionHappened = false;
    }

    /**
     * Requests this HAcidDelete operation to remove the value of the specified column and value.
     *
     * @param family
     * @param qualifier
     * @return
     */
    public HAcidDelete specifyColumn(String family, String qualifier) throws IOException {
        return specifyColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * Requests this HAcidDelete operation to remove the value of the specified column and value.
     *
     * @param family
     * @param qualifier
     * @return
     */
    public HAcidDelete specifyColumn(byte[] family, byte[] qualifier) throws IOException {
        if(executionHappened) {
            LOG.warn("Can't specify columns to delete on a HAcid delete that "
                + "has been already executed"+this.toString()
            );
            return this;
        }

        this.hasSpecifiedColumns = true;
        _hacidput.add(family, qualifier, Schema.CELL_VALUE_EMPTY);
        return this;
    }

    public byte[] getRow() {
        return row;
    }

    /**
     * Performs the conventional HTable operation of deleting the row specified
     * by this HAcidDelete.
     * ATTENTION: this is a dangerous operation, might break the schema of committed
     * timestamps in the row.
     *
     * Deprecated because doesn't seem necessary.
     */
    @Deprecated
    void executeStrictDelete() throws IOException {
        Delete d = new Delete(row);
        hacidtable.delete(d);
    }

    @Override
    void execute(long timestamp) throws IOException {
        if(!hasSpecifiedColumns) {
            // Make all columns have empty value
            Get get = new Get(row);
            Result result = hacidtable.get(get);
            for(KeyValue kv : result.raw()) {
                // TODO make unit test to check that HAcid family is not made empty
                if(!Bytes.equals(kv.getFamily(),Schema.FAMILY_HACID)) {
                    specifyColumn(kv.getFamily(), kv.getQualifier());
                }
            }
        }

        executionHappened = true;
        _hacidput.execute(timestamp);
    }

    @Override
    void rollforward(long end_ts) throws IOException {
        _hacidput.rollforward(end_ts);
    }

    @Override
    void rollback() throws IOException {
        _hacidput.rollback();
    }

    @Override
    public String toString() {
        return " (del#"+super.hashCode()+")";
    }
}
