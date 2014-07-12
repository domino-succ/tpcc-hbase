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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This encapsulates a HBase user table ready for use in HAcid. In other words,
 * a HAcidTable is always ready for use in HAcid because it makes the necessary
 * changes on the given user table.
 *
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidTable {

    protected HTable htable;

    public HAcidTable(final String tableName) throws IOException {
        this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
    }

    public HAcidTable(final byte [] tableName) throws IOException {
        this(HBaseConfiguration.create(), tableName);
    }

    public HAcidTable(Configuration conf, final String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    public HAcidTable(Configuration conf, final byte [] tableName) throws IOException {
        htable = new HTable(conf, tableName);
        if(!HAcidClient.isUserTablePrepared(htable)) {
            // TODO: log info about starting to prepare
            HAcidClient.prepareUserTable(htable);
            // TODO: log info about preparation done
        }
    }

    Result get(Get get) throws IOException {
        return htable.get(get);
    }

    public byte[] getTableName() {
        return htable.getTableName();
    }

    String getTableNameString() {
        return Bytes.toString(htable.getTableName());
    }

    void put(Put put) throws IOException {
        htable.put(put);
    }

    void delete(Delete delete) throws IOException {
        htable.delete(delete);
    }

    public void close() throws IOException {
        htable.close();
    }

}
