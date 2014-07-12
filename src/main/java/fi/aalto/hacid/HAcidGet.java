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
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A read operation that can be included into a HAcidTxn.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidGet {

    static Logger LOG = Logger.getLogger(HAcidGet.class.getName());

    HAcidTable hacidtable;
    protected byte[] row;
    protected LinkedList<byte[]> families;
    protected LinkedList<byte[]> qualifiers;

    public HAcidGet(HAcidTable table, byte[] row) {
        this.hacidtable = table;
        this.row = row;
        this.families = new LinkedList<byte[]>();
        this.qualifiers = new LinkedList<byte[]>();
    }

    /**
     * Get the column from the specific family with the specified qualifier.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public HAcidGet addColumn(String family, String qualifier) {
        return addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

    /**
     * Get the column from the specific family with the specified qualifier.
     *
     * @param family
     * @param qualifier
     * @param value
     * @return
     */
    public HAcidGet addColumn(byte[] family, byte[] qualifier) {
        if( Bytes.equals(family,Schema.FAMILY_HACID) ) {
            LOG.warn("It is not recommended to do a HAcidGet on a HAcid metadata column");
        }

        families.add(family);
        qualifiers.add(qualifier);
        
        return this;
    }

    public byte[] getRow() {
        return row;
    }

    Result collectReadData(Result resultIn, long timestamp) {
        LinkedList<KeyValue> collectedKeyValues = new LinkedList<KeyValue>();

        Result resultOut = new Result(resultIn.raw());

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
            resultOut.getMap()
        ;
        while(!map.isEmpty()) {
            Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> aFamily =
                map.pollFirstEntry()
            ;
            while(aFamily != null && !aFamily.getValue().isEmpty()) {
                Entry<byte[], NavigableMap<Long, byte[]>> aQualifier =
                    aFamily.getValue().pollFirstEntry()
                ;
                versionsLoop: while(aQualifier != null && !aQualifier.getValue().isEmpty()) {
                    Entry<Long, byte[]> aVersion =
                        aQualifier.getValue().pollFirstEntry()
                    ;
                    if(aVersion.getKey() <= timestamp) {
                        collectedKeyValues.add(new KeyValue(
                            resultIn.getRow(),
                            aFamily.getKey(),
                            aQualifier.getKey(),
                            aVersion.getKey(),
                            aVersion.getValue()
                        ));
                        break versionsLoop;
                    }
                }
            }
        }

        return (new Result(collectedKeyValues));
    }

    /**
     * Add columns of this HAcidGet to the specified HBase Get.
     *
     * @param g The HBase Get that will receive this instance's column data.
     * @return
     */
    Get sendToGet(Get g) {
        for(int i=0; i<families.size(); i++) {
            g.addColumn(families.get(i), qualifiers.get(i));
        }
        return g;
    }

    /**
     * Rebuilds part of the data of a HAcidGet given a readset string of the
     * form "tableName:rowName".
     *
     * @param readEntry
     * @param conf
     * @return
     * @throws IOException
     */
    static HAcidGet restore(
        String readEntry,
        Configuration conf) throws IOException
    {
        String tablename = readEntry.substring(0, readEntry.indexOf(":"));
        String rowname   = readEntry.substring(readEntry.indexOf(":")+1);

        HAcidGet pget = new HAcidGet(
            new HAcidTable(conf, tablename),
            Bytes.toBytes(rowname)
        );

        return pget;
    }

    @Override
    public String toString() {
        String str = "[HAcidGet from table "+this.hacidtable.getTableNameString()
            +" row "+Bytes.toString(this.row)+" columns: "
        ;

        for(int i=0; i<families.size(); i++) {
            str = str.concat(Bytes.toString(families.get(i))
                    +":"+Bytes.toString(qualifiers.get(i))
                    +" "
            );
        }
        str = str.concat("]");

        return str;
    }
}
