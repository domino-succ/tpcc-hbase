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
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Provides interaction with a HAcid system in a HBase site, offering
 * transaction processing.
 * 
 * <p>
 * Example:
 * 
 * <p>
 * <code>
 * Configuration conf = HBaseConfiguration.create();<br />
 * HAcidClient client = new HAcidClient(conf); <br />
 * HAcidTable mytable = new HAcidTable(conf, "mytable"); <br />
 * <br />
 * HAcidTxn txn = new HAcidTxn(client); <br />
 * HAcidPut p1 = new HAcidPut(mytable, Bytes.toBytes("row1")); <br />
 * p1.add(Bytes.toBytes("fam"),Bytes.toBytes("col1"),Bytes.toBytes("some_value")); <br />
 * HAcidPut p2 = new HAcidPut(mytable, Bytes.toBytes("row2")); <br />
 * p2.add(Bytes.toBytes("fam"),Bytes.toBytes("col1"),Bytes.toBytes("some_other_value")); <br />
 * txn.put(p1); <br />
 * txn.put(p2); <br />
 * txn.commit(); <br />
 * <br />
 * // ...<br />
 * <br />
 * mytable.close(); <br />
 * client.close();<br />
 * </code>
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidClient {

  static Logger LOG = Logger.getLogger(HAcidClient.class.getName());

  private HTable timestamp_log_table;
  Configuration configuration_HBase;
  private static int SCAN_TIMESTAMP_MAX_CACHE = 1024; // max num. rows in scan

  public enum IsolationLevel {
    /**
     * Snapshot Isolation is an Isolation Level where reads in transactions
     * operate on a "snapshot" of all data at the beginning of the transaction.
     * This Isolation Level does not necessarily produce serializable histories. <br />
     * <br />
     * This is the default Isolation Level in HAcid.
     */
    SNAPSHOT_ISOLATION,
    /**
     * Write-Snapshot Isolation ("A Critique of Snapshot Isolation", Ferro &
     * Yabandeh) is an Isolation Level that provides Serializability. Operates
     * in a similar way as Snapshot Isolation, but detects read-write conflicts
     * instead of write-write conflicts. Gives Serializability at the cost of
     * minor performance drawbacks.
     */
    WRITE_SNAPSHOT_ISOLATION;
  }

  private IsolationLevel isolation = IsolationLevel.SNAPSHOT_ISOLATION;

  /**
   * @return the Isolation Level to deal with transactions.
   */
  public IsolationLevel getIsolation() {
    return isolation;
  }

  /**
   * Sets the isolation level for transactions in HAcid. <br />
   * <br />
   * WARNING: it is the programmer's responsibility to ensure that all
   * concurrent HAcid clients are using the same isolation level. Moreover, The
   * programmer is also responsible for ensuring that no HAcid clients are
   * actively managing transactions when the isolation level is modified.
   * Otherwise, some transactions may lose their consistency and isolation
   * properties. HAcid does not perform distributed consensus of the new
   * isolation level. <br />
   * <br />
   * Therefore, it is recommended to set the isolation level only at the
   * beginning of HAcid's use.
   * 
   * @param isolation
   *          the Isolation Level to deal with transactions.
   */
  public void setIsolation(IsolationLevel isolation) {
    this.isolation = isolation;
  }

  /**
   * Initializes a HAcid client instance. If necessary, installs HAcid in the
   * HBase site, creating the HAcid metadata tables.
   * 
   * @param config
   *          The configuration data of the HBase site where HAcid metadata
   *          tables will stay.
   * @throws Exception
   */
  public HAcidClient(Configuration config) throws Exception {
    if (!isInstalled(config)) {
      install(config, null);
    }
    initialize(config);
  }

  /**
   * Initializes a HAcid client instance. If necessary, installs HAcid in the
   * HBase site, creating the HAcid metadata tables, and prepares each user
   * table given by <code>allUserTables</code> to be used by HAcid.
   * 
   * @param config
   *          The configuration data of the HBase site where HAcid metadata
   *          tables will stay.
   * @param allUserTables
   *          A Collection of user tables to be prepared for use in HAcid.
   * @throws Exception
   */
  public HAcidClient(Configuration config, Collection<HTable> allUserTables)
      throws Exception {
    if (!isInstalled(config)) {
      install(config, allUserTables);
    }
    initialize(config);
  }

  /**
   * Prepare this client for using HAcid metadata tables at HBase site given by
   * config.
   * 
   * @throws Exception
   */
  private void initialize(Configuration config) throws Exception {
    LOG.debug("Initializing HAcid client ...");

    if (config == null) {
      LOG.error("No connection to HBase. Configuration is null");
    }
    configuration_HBase = config;

    if (timestamp_log_table != null) {
      timestamp_log_table.close();
      timestamp_log_table = null;
    }
    timestamp_log_table = new HTable(config, Schema.TABLE_TIMESTAMP_LOG);
  }

  /**
   * At the end of this HAcid client session, call this to properly end the
   * client interaction.
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    LOG.debug("Closing HAcid client ...");

    timestamp_log_table.close();
  }

  static byte[] timestampToKey(byte[] timestampBytes) {
    return timestampToKey(Bytes.toLong(timestampBytes));
  }

  static byte[] timestampToKey(long timestamp) {
    return Bytes.toBytes((Long.MAX_VALUE - timestamp));
  }

  static long keyToTimestamp(byte[] timestamp_log_row_key) {
    return keyToTimestamp(Bytes.toLong(timestamp_log_row_key));
  }

  static long keyToTimestamp(long timestamp_log_row_key) {
    return (Long.MAX_VALUE - timestamp_log_row_key);
  }

  /**
   * This describes a Put to be performed atomically at the same time HAcid
   * retrieves a timestamp.
   */
  private interface ActionOnGrab {
    Put makePut(byte[] rowkey);
  }

  /**
   * Describes what kind of Put should be done on the row that gives a new start
   * timestamp.
   */
  private class StartTimestampActionOnGrab implements ActionOnGrab {
    @Override
    public Put makePut(byte[] rowkey) {
      Put p = new Put(rowkey);
      p.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TS_TYPE, Schema.TYPE_START);
      p.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE,
          Schema.STATE_ACTIVE);
      return p;
    }
  }

  /**
   * Describes what kind of Put should be done on the Timestamp-Log row that
   * gives a new end timestamp.
   */
  private class EndTimestampActionOnGrab implements ActionOnGrab {
    private HAcidTxn _txn;

    public EndTimestampActionOnGrab(HAcidTxn txn) {
      _txn = txn;
    }

    @Override
    public Put makePut(byte[] rowkey) {
      Put p = new Put(rowkey);

      p.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TS_TYPE, Schema.TYPE_END);

      p.add(Schema.FAMILY_HACID, Schema.QUALIFIER_START_TIMESTAMP,
          Bytes.toBytes(_txn.getStartTimestamp()));

      Collection<byte[]> writesetRows = _txn.getWriteset();
      for (byte[] rowId : writesetRows) {
        LOG.trace(_txn.toStringHash() + " writes cell " + Bytes.toString(rowId));
        p.add(Schema.FAMILY_WRITESET, rowId, Schema.MARKER_TRUE);
      }

      Collection<byte[]> readsetRows = _txn.getReadset();
      for (byte[] rowId : readsetRows) {
        LOG.trace(_txn.toStringHash() + " reads cell " + Bytes.toString(rowId));
        p.add(Schema.FAMILY_READSET, rowId, Schema.MARKER_TRUE);
      }

      return p;
    }
  }

  /**
   * Retrieves a new start timestamp from HAcid for some transaction, and
   * (atomically) at the same time registers the 'active' state for that txn.
   * 
   * @return
   * @throws IOException
   */
  long requestStartTimestamp() throws IOException {
    return grabTimestamp(new StartTimestampActionOnGrab());
  }

  /**
   * Retrieves an end timestamp from HAcid for the transaction <code>txn</code>,
   * and (atomically) at the same time registers the write set of
   * <code>txn</code>.
   * 
   * @param txn
   * @return
   * @throws IOException
   */
  long requestEndTimestamp(HAcidTxn txn) throws IOException {
    return grabTimestamp(new EndTimestampActionOnGrab(txn));
  }

  /**
   * How many steps is the grabTimestamp method allowed to take before skipping
   * to the beginning of the Timestamp Log. A 'step' is a failed checkAndPut for
   * a timestamp one row above the previous attempt.
   */
  public static int GRAB_TIMESTAMP_MAX_STEPS_BEFORE_SKIPPING_TO_BEGINNING = 26;

  // 26 was determined by experimenting values, and it is a local minimum
  // i.e., 26 renders a local minimum of number of invocations to checkAndPut,
  // and also average time spent in grabTimestamp.

  /**
   * Atomically creates a new timestamp in HAcid and applies
   * <code>postGrabPut</code> on that timestamp.
   * 
   * @param postGrabPut
   * @return
   * @throws IOException
   */
  private long grabTimestamp(ActionOnGrab action) throws IOException {
    if (timestamp_log_table == null) {
      LOG.error("HAcid Timestamp-Log table is not initialized");
    }
    if (action == null) {
      LOG.error("Null ActionOnGrab given to grabTimestamp()");
    }

    long firstRowKey = Bytes
        .toLong(timestampToKey(Schema.TIMESTAMP_INITIAL_LONG));
    int steps = GRAB_TIMESTAMP_MAX_STEPS_BEFORE_SKIPPING_TO_BEGINNING;

    boolean firstIsAvailable; // "available" means "inexistent row"
    do {
      if (steps >= GRAB_TIMESTAMP_MAX_STEPS_BEFORE_SKIPPING_TO_BEGINNING) {
        steps = 0;
        // Get the first row
        Scan scan = new Scan();
        ResultScanner scanner = timestamp_log_table.getScanner(scan);
        Result firstRowRes = scanner.next();
        scanner.close();
        scanner = null;
        scan = null;
        if (firstRowRes == null) {
          LOG.fatal("HAcid Timestamp Log is empty.");
        }
        firstRowKey = Bytes.toLong(firstRowRes.getRow()) - 1L;
      }

      // Checks if the first row is available, and if true does the Put
      firstIsAvailable = timestamp_log_table.checkAndPut(
          Bytes.toBytes(firstRowKey), Schema.FAMILY_HACID,
          Schema.QUALIFIER_TS_TYPE, null,
          action.makePut(Bytes.toBytes(firstRowKey)));

      // Prepares to retry to grab the timestamp from a newly created row
      if (!firstIsAvailable) {
        steps++;
        firstRowKey--;
        if (firstRowKey <= 0L) {
          LOG.fatal("Ran out of timestamps.");
        }
      }
    }
    while (!firstIsAvailable);

    return keyToTimestamp(firstRowKey);
  }

  /**
   * Returns row data at the proper timestamp with regard to the reference
   * timestamp given.
   * 
   * If the Isolation Level is Snapshot Isolation, returns row data at the S.I.
   * Read Timestamp of the given <code>row</code> respecting the reference
   * timestamp <code>ref_timestamp</code>.
   * 
   * <p>
   * Given the reference timestamp <code>ref_timestamp</code> (usually the
   * start-timestamp of the txn R that is performing the read), and a user data
   * row <code>row</code>, the Snapshot Isolated Read Timestamp is defined as
   * T.start-ts of the transaction T that wrote to <code>row</code>, with
   * T.end-ts as close as possible to <code>ref_timestamp</code>, but still
   * T.end-ts < <code>ref_timestamp</code>. If R wrote to <code>row</code>
   * before doing this search, then Snapshot Isolated Read Timestamp is defined
   * as R.start-ts.
   * 
   * @param usertable
   * @param row
   * @param timestamp
   * @return
   * @throws IOException
   */
  Result getFromReadTimestamp(HAcidGet hacidget, long ref_timestamp)
      throws IOException {
    LOG.debug("Getting Snapshot Isolated Read Timestamp of "
        + Bytes.toString(hacidget.hacidtable.getTableName()) + ":"
        + Bytes.toString(hacidget.getRow()) + " until timestamp "
        + ref_timestamp);

    // Get the specified user row, all timestamps
    Get get = new Get(hacidget.getRow());
    get.setMaxVersions(); // get all versions
    hacidget.sendToGet(get); // add user columns
    get.addColumn(Schema.FAMILY_HACID, Schema.QUALIFIER_USERTABLE_COMMITTED_AT); // add
                                                                                 // txn
                                                                                 // metadata
                                                                                 // column
    Result res = hacidget.hacidtable.get(get);
    List<KeyValue> list_committed_at = res.getColumn(Schema.FAMILY_HACID,
        Schema.QUALIFIER_USERTABLE_COMMITTED_AT);

    LOG.trace("Timestamp given: " + ref_timestamp);

    long best_timestamp = 0L; // for finding write snapshot isolated t.s.
    long best_committed_at = 0L; // for finding write snapshot isolated t.s.

    for (KeyValue kv : list_committed_at) {
      long cell_timestamp = kv.getTimestamp();
      long cell_committed_at = Bytes.toLong(kv.getValue());

      LOG.trace("ts " + cell_timestamp + ": " + "(committed-at cell = "
          + cell_committed_at + ")");

      // If row is uncommitted, suspect that it should be committed
      if (cell_timestamp != ref_timestamp
          && cell_committed_at == Schema.TIMESTAMP_NULL_LONG) {
        // Get the state of its parent txn from Timestamp-Log
        Result cell_startTS_res = timestamp_log_table.get(new Get(
            timestampToKey(cell_timestamp)));
        byte[] otherTxnState = cell_startTS_res.getColumnLatest(
            Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE).getValue();

        // State of parent txn active (= before commit/abort decision)
        if (Bytes.equals(otherTxnState, Schema.STATE_ACTIVE)) {
          // Scan from cell_timestamp to ref_timestamp in Timestamp Log
          // and find the row that has start-ts = cell_timestamp
          SingleColumnValueFilter filter = new SingleColumnValueFilter(
              Schema.FAMILY_HACID, Schema.QUALIFIER_START_TIMESTAMP,
              CompareFilter.CompareOp.EQUAL, Bytes.toBytes(cell_timestamp));
          filter.setFilterIfMissing(true);
          Scan scan = new Scan(timestampToKey(ref_timestamp - 1L),
              timestampToKey(cell_timestamp));
          int cacheSize = ((ref_timestamp - cell_timestamp) < SCAN_TIMESTAMP_MAX_CACHE) ? ((int) (ref_timestamp - cell_timestamp))
              : SCAN_TIMESTAMP_MAX_CACHE;
          scan.setCaching(cacheSize);
          scan.setFilter(filter);
          ResultScanner scanner = timestamp_log_table.getScanner(scan);
          Result cell_endTS_res = scanner.next();
          scanner.close();

          // If found that end timestamp
          if (cell_endTS_res != null) {
            // do recovery from canCommit() onwards (like decideCommitOrAbort)
            HAcidTxn cellTxn = HAcidTxn.restore(this, cell_endTS_res);
            if (cellTxn.decideCommitOrAbort() == true) {
              cell_committed_at = cellTxn.getEndTimestamp();
            }
          }
          else { // not found that commit timestamp
            // Do nothing because the parent of cell_timestamp will
            // have commit timestamp larger than the ref_timestamp
            continue;
          }
        }
        else { // Decision abort/commit is already taken, so do recovery
          Result cell_endTS_res = timestamp_log_table.get(new Get(
              timestampToKey(cell_startTS_res.getColumnLatest(
                  Schema.FAMILY_HACID, Schema.QUALIFIER_END_TIMESTAMP)
                  .getValue())));
          HAcidTxn cellTxn = HAcidTxn.restore(this, cell_endTS_res);

          // if decision was abort, then rollback
          if (Bytes.equals(otherTxnState, Schema.STATE_ABORTED)) {
            cellTxn.rollbackWrites();
            continue;
          }
          // if decision was commit, then commit the operations
          else if (Bytes.equals(otherTxnState, Schema.STATE_COMMITTED)) {
            cellTxn.rollforwardWrites();
            cell_committed_at = cellTxn.getEndTimestamp();
          }
        }
      }

      if (
      // ( // txn is allowed to read its own modifications
      // cell_timestamp == ref_timestamp
      // )
      // ||
      ( // for Snapshot Isolation
      cell_timestamp < ref_timestamp
          && cell_committed_at != Schema.TIMESTAMP_NULL_LONG && cell_committed_at < ref_timestamp)) {
        if (isolation == IsolationLevel.SNAPSHOT_ISOLATION) {
          LOG.trace(cell_timestamp + " is the Snapshot Isolated Read Timestamp");
          return hacidget.collectReadData(res, cell_timestamp);
        }
        else if (isolation == IsolationLevel.WRITE_SNAPSHOT_ISOLATION) {
          if (cell_committed_at > best_committed_at) {
            best_timestamp = cell_timestamp;
            best_committed_at = cell_committed_at;
          }
        }
      }
    }// end for

    if (isolation == IsolationLevel.WRITE_SNAPSHOT_ISOLATION
        && best_timestamp > 0) {
      LOG.trace(best_timestamp
          + " is the Write Snapshot Isolated Read Timestamp");
      return hacidget.collectReadData(res, best_timestamp);
    }

    // Something unusual happened:
    return (new Result(new LinkedList<KeyValue>()));
  }

  /**
   * Retrieves data of a timestamp from the Timestamp Log.
   * 
   * @param timestamp
   *          the timestamp itself
   * @return
   */
  Result getTimestampData(long timestamp) throws IOException {
    Get g = new Get(timestampToKey(timestamp));
    return timestamp_log_table.get(g);
  }

  /**
   * Minimum time, in milliseconds, to wait when a previous transaction with
   * state 'active' is encountered, while checking if a subsequent transaction
   * can commit.
   */
  public static long MIN_TIMEOUT_WAIT_PREVIOUS_ACTIVE_TXN = 128L;

  /**
   * Maximum time, in milliseconds, to wait when a previous transaction with
   * state 'active' is encountered, while checking if a subsequent transaction
   * can commit.
   */
  public static long MAX_TIMEOUT_WAIT_PREVIOUS_ACTIVE_TXN = 16384L;

  /**
   * Number of attempts to recheck for transaction conflicts when previous
   * active transactions are found.
   */
  public static final int DEFAULT_CONFLICT_TEST_ATTEMPTS = 5;

  boolean canCommit(HAcidTxn txn) throws IOException {
    return canCommit(txn, DEFAULT_CONFLICT_TEST_ATTEMPTS);
  }

  /**
   * Tells whether HAcid can commit or not the given transaction, i.e., whether
   * the transaction <code>txn</code> conflicts with some other registered
   * transaction.
   * 
   * @param txn
   * @param attempts
   *          Number of attempts to recheck for transaction conflicts when
   *          previous active transactions are found.
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  boolean canCommit(HAcidTxn txn, int attempts) throws IOException {
    LOG.info("Checking for conflicts" + txn.toStringHash());

    // No timestamps between txn's start_ts and end_ts
    if (txn.getStartTimestamp() + 1L == txn.getEndTimestamp()) {
      return true;
    }

    int fixed_attempts = (attempts >= 1) ? attempts : 1;

    // From the Timestamp Log, get the end timestamp row of the current txn
    Get g = new Get(timestampToKey(txn.getEndTimestamp()));
    if (isolation == IsolationLevel.SNAPSHOT_ISOLATION) {
      g.addFamily(Schema.FAMILY_WRITESET);
    }
    else if (isolation == IsolationLevel.WRITE_SNAPSHOT_ISOLATION) {
      g.addFamily(Schema.FAMILY_READSET);
    }
    // this will either hold the writeset or the readset,
    // according to the isolation level:
    Result currentTxnComparisonSet = timestamp_log_table.get(g);

    // Scan previous possibly conflicting transactions before 'txn'
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        Schema.FAMILY_HACID, Schema.QUALIFIER_TS_TYPE,
        CompareFilter.CompareOp.EQUAL, Schema.TYPE_END);
    filter.setFilterIfMissing(true);
    Scan scan = new Scan(timestampToKey(txn.getEndTimestamp() - 1L),
        timestampToKey(txn.getStartTimestamp()));
    int cacheSize = ((txn.getEndTimestamp() - txn.getStartTimestamp()) < SCAN_TIMESTAMP_MAX_CACHE) ? ((int) (txn
        .getEndTimestamp() - txn.getStartTimestamp()))
        : SCAN_TIMESTAMP_MAX_CACHE;
    scan.setCaching(cacheSize);
    scan.setFilter(filter); // get only timestamp rows that are end timestamps
    Result previousTxn;
    long timeout = MIN_TIMEOUT_WAIT_PREVIOUS_ACTIVE_TXN;

    for (int i = 0; i < fixed_attempts; i++) {
      ResultScanner scanner = timestamp_log_table.getScanner(scan);
      // No other transaction committed in my life span, hence I can commit
      if ((previousTxn = scanner.next()) == null) {
        LOG.debug("No other txn committed during " + txn.toString()
            + ", so it can commit" + txn.toStringHash());
        return true;
      }
      // There might be conflicts:
      else {
        boolean foundActiveConflictingTxn = false;
        do {
          // Fetch state of the previous txn from its start-ts row
          byte[] stateOfPrevious = timestamp_log_table
              .get(
                  new Get(timestampToKey(previousTxn.getColumnLatest(
                      Schema.FAMILY_HACID, Schema.QUALIFIER_START_TIMESTAMP)
                      .getValue())).addColumn(Schema.FAMILY_HACID,
                      Schema.QUALIFIER_TXN_STATE))
              .getColumnLatest(Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE)
              .getValue();

          // Search for conflicts only with 'committed' and 'active'
          // transactions
          if (!Bytes.equals(stateOfPrevious, Schema.STATE_ABORTED)) {
            intersectionCheck: // Compare the previous and the current txns
            for (KeyValue kv : currentTxnComparisonSet.raw()) {
              LOG.trace("current txn has "
                  + Bytes.toString(Schema.FAMILY_WRITESET) + ":"
                  + Bytes.toString(kv.getQualifier()) + txn.toStringHash());
              if (previousTxn.containsColumn(Schema.FAMILY_WRITESET,
                  kv.getQualifier())) {
                if (Bytes.equals(stateOfPrevious, Schema.STATE_COMMITTED)) {
                  LOG.warn("TRANSACTION CONFLICT: Both transactions "
                      + HAcidTxn.lightRestore(this, previousTxn).toString()
                      + " and " + txn.toString() + " modify the common cell "
                      + Bytes.toString(kv.getQualifier())
                      + ", so the second txn must abort." + txn.toStringHash());
                  scanner.close();
                  return false; // Found a conflict so cannot commit
                }
                else if (Bytes.equals(stateOfPrevious, Schema.STATE_ACTIVE)) {
                  LOG.warn("Found a previous active conflicting transaction "
                      + HAcidTxn.lightRestore(this, previousTxn).toString()
                      + txn.toStringHash());
                  foundActiveConflictingTxn = true; // Found an active
                                                    // conflicting txn
                  break intersectionCheck;
                }
              }
            }
          }
        }
        while ((previousTxn = scanner.next()) != null);

        // No conflicts found and no previous active conflicting txn, so can
        // commit
        if (!foundActiveConflictingTxn) {
          LOG.debug("No conflicting transactions found, so it can commit"
              + txn.toStringHash());
          scanner.close();
          return true;
        }
      }
      scanner.close();

      // Wait until some previous 'active' transaction gets decided:
      LOG.debug("Waiting " + timeout + " ms until previous active "
          + "conflicting txns get decided" + txn.toStringHash());
      try {
        Thread.sleep(timeout);
      }
      catch (InterruptedException ex) {
        LOG.warn(ex.toString());
      }
      timeout *= 2;
      if (timeout > MAX_TIMEOUT_WAIT_PREVIOUS_ACTIVE_TXN) {
        timeout = MAX_TIMEOUT_WAIT_PREVIOUS_ACTIVE_TXN;
      }
    }

    LOG.debug("Last check for conflicts, after the timeout"
        + txn.toStringHash());
    // Try to check again
    ResultScanner scannerAgain = timestamp_log_table.getScanner(scan);
    while ((previousTxn = scannerAgain.next()) != null) {
      byte[] stateOfPrevious = timestamp_log_table
          .get(
              new Get(timestampToKey(previousTxn.getColumnLatest(
                  Schema.FAMILY_HACID, Schema.QUALIFIER_START_TIMESTAMP)
                  .getValue())).addColumn(Schema.FAMILY_HACID,
                  Schema.QUALIFIER_TXN_STATE))
          .getColumnLatest(Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE)
          .getValue();

      // Conflicts with 'active' or 'committed' transactions are enough to abort
      if (!Bytes.equals(stateOfPrevious, Schema.STATE_ABORTED)) {
        for (KeyValue kv : currentTxnComparisonSet.raw()) {
          if (previousTxn.containsColumn(Schema.FAMILY_WRITESET,
              kv.getQualifier())) {
            LOG.warn("TRANSACTION CONFLICT: Both transactions "
                + HAcidTxn.lightRestore(this, previousTxn).toString() + " and "
                + txn.toString() + " modify the common cell "
                + Bytes.toString(kv.getQualifier())
                + ", so the second txn must abort." + txn.toStringHash());
            scannerAgain.close();
            return false; // Found a conflict so cannot commit
          }
        }
      }
    }

    // No conflicts found, so can commit
    LOG.debug("No conflicting transactions found, so it can commit");
    scannerAgain.close();
    return true;
  }

  /**
   * Sets the state of the specified transaction in the Timestamp Log.
   * 
   * @param txn
   * @param newState
   * @throws IOException
   */
  private void setDecisionState(HAcidTxn txn, byte[] newState)
      throws IOException, StateDisagreementException {
    // Change the state in the Timestamp Log
    Put tslog_put = new Put(timestampToKey(txn.getStartTimestamp()));
    tslog_put.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE, newState);
    tslog_put.add(Schema.FAMILY_HACID, Schema.QUALIFIER_END_TIMESTAMP,
        Bytes.toBytes(txn.getEndTimestamp()));
    if (timestamp_log_table.checkAndPut(
        timestampToKey(txn.getStartTimestamp()), Schema.FAMILY_HACID,
        Schema.QUALIFIER_TXN_STATE, Schema.STATE_ACTIVE, tslog_put) == false) {
      throw new StateDisagreementException();
    }
  }

  /**
   * Marks the specified transaction as "committed" in the Timestamp Log.
   * 
   * @param txn
   * @throws IOException
   */
  void setStateCommitted(HAcidTxn txn) throws IOException,
      StateDisagreementException {
    LOG.info("Txn " + txn.toString() + " is COMMITTED");
    setDecisionState(txn, Schema.STATE_COMMITTED);
  }

  /**
   * Marks the specified transaction as "aborted" in the Timestamp Log.
   * 
   * @param txn
   * @throws IOException
   */
  void setStateAborted(HAcidTxn txn) throws IOException,
      StateDisagreementException {
    LOG.info("Txn " + txn.toString() + " is ABORTED");
    setDecisionState(txn, Schema.STATE_ABORTED);
  }

  /**
   * Tells whether or not HAcid is installed at the HBase site referred by
   * config.
   * 
   * @param config
   * @return Whether or not HAcid is installed at the HBase site
   * @throws Exception
   */
  public static boolean isInstalled(Configuration config) throws Exception {
    if (config == null) {
      LOG.error("No connection to HBase. Configuration is null");
    }

    HBaseAdmin admin = new HBaseAdmin(config);

    LOG.trace("Checking whether HAcid is installed in " + config.toString());

    return admin.tableExists(Schema.TABLE_TIMESTAMP_LOG);
  }

  /**
   * Sets up a HBase data store to use HAcid. Creates the HAcid metadata tables,
   * and prepares each user table to be used by HAcid.
   * 
   * @param config
   *          The configuration data of the HBase site where HAcid metadata
   *          tables will stay.
   * @param userTables
   *          A Collection of user tables to be prepared for use in HAcid.
   * @throws Exception
   */
  private static void install(Configuration config,
      Collection<HTable> allUserTables) throws Exception {
    if (config == null) {
      LOG.error("No connection to HBase. Configuration is null");
    }

    HBaseAdmin admin = new HBaseAdmin(config);

    LOG.info("Installing HAcid in " + config.toString() + " ...");

    // Opens the Timestamp Log table, and first initializes it if necessary
    if (!admin.tableExists(Schema.TABLE_TIMESTAMP_LOG)) {
      LOG.debug("Creating HAcid Timestamp Log table");
      HTableDescriptor logDescriptor = new HTableDescriptor(
          Schema.TABLE_TIMESTAMP_LOG);
      HColumnDescriptor hacidFamily = new HColumnDescriptor(Schema.FAMILY_HACID);
      HColumnDescriptor writesetFamily = new HColumnDescriptor(
          Schema.FAMILY_WRITESET);
      HColumnDescriptor readsetFamily = new HColumnDescriptor(
          Schema.FAMILY_READSET);
      hacidFamily.setMaxVersions(1);
      writesetFamily.setMaxVersions(1);
      readsetFamily.setMaxVersions(1);
      logDescriptor.addFamily(hacidFamily);
      logDescriptor.addFamily(writesetFamily);
      logDescriptor.addFamily(readsetFamily);
      admin.createTable(logDescriptor);
      HTable logTable = new HTable(config, Schema.TABLE_TIMESTAMP_LOG);

      // Makes the first timestamp as used (because user tables might be
      // initialized with this)
      Put firstPut = new Put(timestampToKey(Schema.TIMESTAMP_INITIAL_LONG));
      firstPut.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TS_TYPE,
          Schema.TYPE_END);
      firstPut.add(Schema.FAMILY_HACID, Schema.QUALIFIER_START_TIMESTAMP,
          Schema.TIMESTAMP_INITIAL);
      firstPut.add(Schema.FAMILY_HACID, Schema.QUALIFIER_END_TIMESTAMP,
          Schema.TIMESTAMP_INITIAL);
      firstPut.add(Schema.FAMILY_HACID, Schema.QUALIFIER_TXN_STATE,
          Schema.STATE_COMMITTED);
      logTable.put(firstPut);
    }

    // Prepares all user tables
    if (allUserTables != null) {
      LOG.debug("Preparing a collection of user tables for use in HAcid");
      for (HTable usertable : allUserTables) {
        prepareUserTable(usertable);
      }
    }
  }

  /**
   * Removes the HAcid metadata tables from HBase.
   * 
   * @param config
   *          The configuration data of the HBase site where HAcid metadata
   *          tables are.
   * @throws Exception
   */
  public static void uninstall(Configuration config) throws Exception {
    if (config == null) {
      LOG.error("No connection to HBase. Configuration is null");
    }

    LOG.debug("Uninstalling HAcid from " + config.toString() + " ...");

    HBaseAdmin admin = new HBaseAdmin(config);
    admin.disableTable(Schema.TABLE_TIMESTAMP_LOG);
    admin.deleteTable(Schema.TABLE_TIMESTAMP_LOG);
  }

  /**
   * Tells whether or not the user table given is prepared for HAcid use.
   * 
   * @param usertable
   * @return
   * @throws Exception
   */
  static boolean isUserTablePrepared(HTable usertable) throws IOException {
    boolean result = (usertable.getTableDescriptor()
        .hasFamily(Schema.FAMILY_HACID));
    LOG.debug("Is user table \'" + Bytes.toString(usertable.getTableName())
        + "\' prepared? " + result);
    return result;
  }

  /**
   * Prepares the user table for HAcid transactions. The user table should not
   * use a column family named 'HAcid'.
   * 
   * ATTENTION: this deletes all previous timestamps of all cells, and keeps the
   * latest timestamp.
   * 
   * @param usertable
   * @throws IOException
   * @throws Exception
   */
  static void prepareUserTable(HTable usertable) throws IOException {
    LOG.info("Preparing user table \'"
        + Bytes.toString(usertable.getTableName()) + "\' for use with HAcid.");

    // Make sure this user table is not already prepared
    if (usertable.getTableDescriptor().getFamily(Schema.FAMILY_HACID) != null) {
      LOG.warn("User table \'" + Bytes.toString(usertable.getTableName())
          + "\' is already prepared for HAcid or uses a column family "
          + "with the special HAcid keyword.");
      return;
    }

    // Prepares admin settings
    if (usertable.getConfiguration() == null) {
      LOG.error("No connection to the HBase site that contains the user "
          + "table. Configuration is null");
    }
    HBaseAdmin admin = new HBaseAdmin(usertable.getConfiguration());
    byte[] usertableName = usertable.getTableName();

    // Set timestamp of all cells to TIMESTAMP_INITIAL
    {
      // Remove all previous data, and remember that data
      Scan scan = new Scan();
      scan.setTimeRange(0L, HConstants.LATEST_TIMESTAMP);
      scan.setMaxVersions();
      ResultScanner scanner = usertable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        LinkedList<Put> reinsertSet = new LinkedList<Put>();
        for (KeyValue kv : result.raw()) {
          if (kv.getTimestamp() != Schema.TIMESTAMP_INITIAL_LONG) {
            Put put = new Put(result.getRow(), Schema.TIMESTAMP_INITIAL_LONG);
            put.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
            reinsertSet.add(put);
            Delete deleteThisVersion = new Delete(result.getRow());
            deleteThisVersion.deleteColumn(kv.getFamily(), kv.getQualifier(),
                kv.getTimestamp());
            usertable.delete(deleteThisVersion);
          }
        }
        // Reinsert all previous data, but with new timestamp
        for (Put reinsert : reinsertSet) {
          usertable.put(reinsert);
        }
        reinsertSet.clear();
        reinsertSet = null;
      }
      scanner.close();
    }

    // Disable the table (necessary before a modify())
    admin.disableTable(usertableName);

    // Set max versions (of each col.family)
    HTableDescriptor tableDescriptor = new HTableDescriptor(usertableName);
    for (HColumnDescriptor previousFamily : admin.getTableDescriptor(
        usertableName).getColumnFamilies()) {
      HColumnDescriptor newFamily = new HColumnDescriptor(previousFamily);
      newFamily.setMaxVersions(Schema.MAX_VERSIONS_USERTABLE);
      tableDescriptor.addFamily(newFamily);
    }

    // Insert new column family 'HAcid'
    HColumnDescriptor hacidFamily = new HColumnDescriptor(Schema.FAMILY_HACID);
    hacidFamily.setMaxVersions(Schema.MAX_VERSIONS_USERTABLE);
    tableDescriptor.addFamily(hacidFamily);

    // Apply the changes to the schema
    admin.modifyTable(usertableName, tableDescriptor);

    // Enable the table
    admin.enableTable(usertableName);

    // Insert the committed-at column for each row
    {
      Scan scan = new Scan();
      ResultScanner scanner = usertable.getScanner(scan);
      Result result;
      while ((result = scanner.next()) != null) {
        Put committed_at_put = new Put(result.getRow(),
            Schema.TIMESTAMP_INITIAL_LONG);
        committed_at_put.add(Schema.FAMILY_HACID,
            Schema.QUALIFIER_USERTABLE_COMMITTED_AT, Schema.TIMESTAMP_INITIAL);
        usertable.put(committed_at_put);
      }
      scanner.close();
    }
    usertable.flushCommits();
  }

}
