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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants to define the names used in tables and column families for HAcid.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class Schema {

    public static final String VERSION = "1.1.5";
    public static final String TIMESTAMP_LOG_VERSION = "1.1.5"; // only change this when the timestamp log's schema is different
    private static final String HASH = Integer.toString(TIMESTAMP_LOG_VERSION.hashCode(), 16).toUpperCase();

    public static final byte[] FAMILY_HACID                     = Bytes.toBytes("HACID"+HASH);

    // Timestamp-Log schema
    public static final byte[] TABLE_TIMESTAMP_LOG              = Bytes.toBytes("__HACID_LOG_"+HASH+"__");
    public static final byte[] QUALIFIER_TS_TYPE                = Bytes.toBytes("type");
        public static final byte[] TYPE_START                   = Bytes.toBytes("start");
        public static final byte[] TYPE_END                     = Bytes.toBytes("end");
    public static final byte[] QUALIFIER_START_TIMESTAMP        = Bytes.toBytes("start-ts");
    public static final byte[] QUALIFIER_END_TIMESTAMP          = Bytes.toBytes("end-ts");
    public static final byte[] QUALIFIER_TXN_STATE              = Bytes.toBytes("state");
        public static final byte[] STATE_ACTIVE                 = Bytes.toBytes("active");
        public static final byte[] STATE_COMMITTED              = Bytes.toBytes("committed");
        public static final byte[] STATE_ABORTED                = Bytes.toBytes("aborted");
    public static final byte[] FAMILY_WRITESET                  = Bytes.toBytes("writeset");
    public static final byte[] FAMILY_READSET                   = Bytes.toBytes("readset");
    public static final byte[] MARKER_TRUE                      = Bytes.toBytes("1");
    public static final byte[] MARKER_FALSE                     = Bytes.toBytes("0");
    public static final long TIMESTAMP_INITIAL_LONG             = 1L;
    public static final byte[] TIMESTAMP_INITIAL                = Bytes.toBytes(TIMESTAMP_INITIAL_LONG);
    public static final long TIMESTAMP_NULL_LONG                = 0L;
    public static final byte[] TIMESTAMP_NULL                   = Bytes.toBytes(TIMESTAMP_NULL_LONG);

    // User table schema
    public static final byte[] CELL_VALUE_EMPTY                 = Bytes.toBytes("");
    public static final int MAX_VERSIONS_USERTABLE              = Integer.MAX_VALUE;
    public static final byte[] QUALIFIER_USERTABLE_COMMITTED_AT = Bytes.toBytes("committed-at");
}
