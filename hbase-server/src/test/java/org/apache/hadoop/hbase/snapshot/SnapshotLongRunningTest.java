/*
 * Operations:
 *  - Take a Snapshot
 *  - Restore
 *  - Clone
 * Tables (different state)
 *  - New Table
 *  - Cloned Table (HFileLink)
 *  - Restore Table (HFile + HFileLink)
 *
 * Configuration Properties for small tests:
 *    - hbase.hstore.compactionThreshold (set high value just to avoid hfilelink to be merged)
 */

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;

public class SnapshotLongRunningTest extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(SnapshotLongRunningTest.class);

  public static class SnapshotContent {
    private final Set<String> families = new HashSet<String>();
    private final Set<String> regions = new HashSet<String>();
    private final Set<String> hfiles = new HashSet<String>();
    private TableInfo tableInfo;

    public void setTableInfo(final TableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
      return this.tableInfo;
    }

    public boolean equals(final SnapshotContent other) {
      if (regions.size() != other.regions.size()) return false;
      if (!families.equals(other.families)) return false;
      if (!hfiles.equals(other.hfiles)) return false;
      return true;
    }

    public void dumpDifferences(final SnapshotContent other) {
      Set<String> s1, s2;

      LOG.warn("regions difference d1=" + this.regions.size() + ", d2=" + other.regions.size());

      s1 = new HashSet<String>(this.families);
      s1.removeAll(other.families);
      s2 = new HashSet<String>(other.families);
      s2.removeAll(this.families);
      LOG.warn("families difference d1=" + s1 + ", d2=" + s2);

      s1 = new HashSet<String>(this.hfiles);
      s1.removeAll(other.hfiles);
      s2 = new HashSet<String>(other.hfiles);
      s2.removeAll(this.hfiles);
      LOG.warn("hfiles difference d1=" + s1 + ", d2=" + s2);
    }

    public static SnapshotContent fromTable(final FileSystem fs, final String tableName)
        throws IOException {
      Path rootDir = FSUtils.getRootDir(fs.getConf());
      Path tableDir = FSUtils.getTablePath(rootDir, tableName);
      return fromTableDir(fs, tableDir);
    }

    public static SnapshotContent fromTableDir(final FileSystem fs, final Path tableDir)
        throws IOException {
      SnapshotContent snapshot = new SnapshotContent();
      FSVisitor.visitTableStoreFiles(fs, tableDir, new StoreFileVisitor(snapshot));
      return snapshot;
    }

    public static SnapshotContent fromSnapshot(final FileSystem fs, final String snapshotName)
        throws IOException {
      Path rootDir = FSUtils.getRootDir(fs.getConf());
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
      return fromSnapshotDir(fs, snapshotDir);
    }

    public static SnapshotContent fromSnapshotDir(final FileSystem fs, final Path snapshotDir)
        throws IOException {
      final SnapshotContent snapshot = new SnapshotContent();
      SnapshotReferenceUtil.visitTableStoreFiles(fs, snapshotDir, new StoreFileVisitor(snapshot));
      return snapshot;
    }

    static class StoreFileVisitor implements FSVisitor.StoreFileVisitor {
      private final SnapshotContent snapshot;

      public StoreFileVisitor(SnapshotContent snapshot) {
        this.snapshot = snapshot;
      }

      public void storeFile(final String regionName, final String familyName,
          final String hfileName) throws IOException {
        snapshot.regions.add(regionName);
        snapshot.families.add(familyName);

        if (HFileLink.isHFileLink(hfileName)) {
          snapshot.hfiles.add(HFileLink.getReferencedHFileName(hfileName));
        } else {
          snapshot.hfiles.add(hfileName);
        }
      }
    }
  }

  public static class TableInfo {
    private String tableName;
    private String checksum;
    private long rowCount;

    public TableInfo(String tableName, long rowCount, String checksum) {
      this.tableName = tableName;
      this.checksum = checksum;
      this.rowCount = rowCount;
    }

    public void dumpDifferences(final TableInfo other) {
      LOG.debug("table info row count d1=" + this.rowCount + ", d2=" + other.rowCount);
      LOG.debug("table info checksum d1=" + this.checksum + ", d2=" + other.checksum);
    }

    public boolean equals(final TableInfo other) {
      return this.rowCount == other.rowCount;
    }

    public String toString() {
      return "TableInfo(tableName=" + tableName + ", rowCount=" +
        rowCount + ", checksum=" + checksum + ")";
    }

    public long getRowCount() {
      return this.rowCount;
    }

    public String getChecksum() {
      return this.checksum;
    }

    public String getTableName() {
      return this.tableName;
    }
  }

  // HBase Cluster Related
  private HTablePool tablePool;
  private HBaseAdmin admin;
  private FileSystem fs;

  // Tables and Snapshots Related
  private Map<String, SnapshotContent> snapshots = new HashMap<String, SnapshotContent>();
  private Map<String, TableInfo> tables = new HashMap<String, TableInfo>();
  private String baseName = "lr_snapshot";
  private long sid = 0;
  private long tid = 0;

  public int run(String[] args) throws Exception {
    for (int i = 0; i < args.length; ++i) {
      printHelpAndQuit();
    }

    // Add to the baseName the current timestamp
    this.baseName += "-" + System.currentTimeMillis();

    // Initialize
    Configuration conf = getConf();
    this.tablePool = new HTablePool(conf, Integer.MAX_VALUE);
    this.admin = new HBaseAdmin(conf);
    this.fs = FileSystem.get(conf);

    final long OPERATION_SLEEP_INTERVAL = 30 * 1000; // sec
    final long TEST_SLEEP_INTERVAL = 5 * 1000; // sec

    final String FAMILY_1 = "f1";
    final String FAMILY_2 = "f2";
    final String TABLE_ONLINE_0 = createTableName();

    try {
      // Create the first test table
      createTable(TABLE_ONLINE_0, FAMILY_1, FAMILY_2);
      loadData(TABLE_ONLINE_0, 2000, true, FAMILY_1, FAMILY_2);

      // Take a snapshot (TODO: Switch to online version)
      String snapshotName = takeOfflineSnapshot(TABLE_ONLINE_0);

      // Clone a table
      String clonedTable = cloneSnapshot(snapshotName);

      // Take a snapshot of the clone
      String cloneSnapshotName = takeOfflineSnapshot(clonedTable);

      // Clone a cloned table snapshot
      String reClonedTable = cloneSnapshot(cloneSnapshotName);

      // Put more data in the table
      loadData(TABLE_ONLINE_0, 4000, true, FAMILY_1, FAMILY_2);
      verifySnapshotAndTableContent(snapshotName, clonedTable);
      verifySnapshotAndTableContent(cloneSnapshotName, reClonedTable);

      // Put more data in the cloned table
      loadData(clonedTable, 4000, true, FAMILY_1, FAMILY_2);
      verifySnapshotAndTableContent(cloneSnapshotName, reClonedTable);

      // Get informations
      updateTableInfo(TABLE_ONLINE_0);
      updateTableInfo(clonedTable);
      updateTableInfo(reClonedTable);

      for (int i = 0; i < 120; ++i) {
        Thread.sleep(TEST_SLEEP_INTERVAL);
        if (Thread.interrupted()) {
          break;
        }

        // Verify and add more data to the original table
        if (!verifyTable(TABLE_ONLINE_0)) {
          throw new Exception("Something wrong with the source table=" + TABLE_ONLINE_0);
        }
        loadData(TABLE_ONLINE_0, 4000, true, FAMILY_1, FAMILY_2);
        updateTableInfo(TABLE_ONLINE_0);
        Thread.sleep(OPERATION_SLEEP_INTERVAL);

        // Verify and add more data to the cloned table
        if (!verifyTable(clonedTable)) {
          throw new Exception("Something wrong with cloned table=" + clonedTable);
        }
        loadData(clonedTable, 4000, true, FAMILY_1, FAMILY_2);
        updateTableInfo(clonedTable);
        Thread.sleep(OPERATION_SLEEP_INTERVAL);

        // Verify and add more data to the clone of the cloned table
        if (!verifyTable(reClonedTable)) {
          throw new Exception("Something wrong with clone of the clone table=" + reClonedTable);
        }
        loadData(reClonedTable, 4000, true, FAMILY_1, FAMILY_2);
        updateTableInfo(reClonedTable);
        Thread.sleep(OPERATION_SLEEP_INTERVAL);

        // Try clone from snapshots
        String testCloneA = cloneSnapshot(snapshotName);
        String testCloneB = cloneSnapshot(cloneSnapshotName);
        verifySnapshotAndTableContent(snapshotName, clonedTable);
        verifySnapshotAndTableContent(cloneSnapshotName, reClonedTable);
        admin.disableTable(testCloneA);
        admin.disableTable(testCloneB);
        admin.deleteTable(testCloneA);
        admin.deleteTable(testCloneB);
      }
    } finally {
      this.tablePool.close();
      this.admin.close();
      this.fs.close();
    }

    return 0;
  }

  private void printHelpAndQuit() {
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err.println();
    System.err.println("Options:");
    System.err.println();
    System.err.println(" Note: -D properties will be applied to the conf used. ");
    System.err.println("  For example: ");
    System.err.println("   -Dhbase.master.cleaner.interval=60000");
    System.out.println();
    System.err.println("Examples:");
    System.err.println(" To run a single evaluation client:");
    System.err.println(" $ bin/hbase " + this.getClass().getName());
    System.exit(1);
  }

  private String takeOfflineSnapshot(final String tableName) throws Exception {
    TableInfo tableInfo = getTableInfo(tableName);
    LOG.debug("Starting offline snapshot of tableInfo=" + tableInfo);
    admin.disableTable(tableName);
    try {
      SnapshotContent tableContent = SnapshotContent.fromTable(fs, tableName);
      String snapshotName = createSnapshotName();
      admin.snapshot(snapshotName, tableName);
      SnapshotContent snapshotContent = SnapshotContent.fromSnapshot(fs, snapshotName);
      if (!snapshotContent.equals(tableContent)) {
        snapshotContent.dumpDifferences(tableContent);
        LOG.error("Snapshot and table content are different!");
        throw new Exception("Snapshot and table content are different!");
      }
      snapshotContent.setTableInfo(tableInfo);
      this.snapshots.put(snapshotName, snapshotContent);
      LOG.debug("Finished offline snapshot=" + snapshotName);
      return snapshotName;
    } catch (IOException e) {
      String msg = "Unable to take offline snapshot of table=" + tableName;
      LOG.error(msg, e);
      throw new Exception(msg);
    } finally {
      admin.enableTable(tableName);
    }
  }

  private void restoreSnapshot(final String snapshotName) throws Exception {
    try {
      String tableName = this.snapshots.get(snapshotName).getTableInfo().getTableName();
      admin.restoreSnapshot(snapshotName);

      if (!verifySnapshotAndTableContent(snapshotName, tableName)) {
        LOG.error("Cloned Snapshot and table content are different!");
        throw new Exception("Cloned Snapshot and table content are different!");
      }
    } catch (IOException e) {
      LOG.error("Unable to restore snapshot=" + snapshotName, e);
    }
  }

  private String cloneSnapshot(final String snapshotName) throws Exception {
    try {
      String tableName = createTableName();
      admin.cloneSnapshot(snapshotName, tableName);

      if (!verifySnapshotAndTableContent(snapshotName, tableName)) {
        LOG.error("Cloned Snapshot and table content are different!");
        throw new Exception("Cloned Snapshot and table content are different!");
      }

      return tableName;
    } catch (IOException e) {
      String msg = "Unable to restore snapshot=" + snapshotName;
      LOG.error(msg, e);
      throw new Exception(msg);
    }
  }

  private boolean verifySnapshotAndTableContent(final String snapshotName, final String tableName)
      throws IOException {
    TableInfo tableInfo = getTableInfo(tableName);
    SnapshotContent snapshotContent = this.snapshots.get(snapshotName);
    SnapshotContent tableContent = SnapshotContent.fromTable(fs, tableName);
    if (!snapshotContent.equals(tableContent) ||
        !tableInfo.equals(snapshotContent.getTableInfo()))
    {
      snapshotContent.dumpDifferences(tableContent);
      tableInfo.dumpDifferences(snapshotContent.getTableInfo());
      return false;
    }
    return true;
  }

  private void updateTableInfo(final String tableName) throws IOException {
    this.tables.put(tableName, getTableInfo(tableName));
  }

  private boolean verifyTable(final String tableName) throws IOException {
    TableInfo prevInfo = this.tables.get(tableName);
    TableInfo currInfo = getTableInfo(tableName);
    if (!currInfo.equals(prevInfo)) {
      currInfo.dumpDifferences(prevInfo);
      LOG.error("Previous and Current state of the table are different table=" + tableName);
      return false;
    }
    return true;
  }

  private String createSnapshotName() {
    return this.baseName + "-snapshot-" + this.sid++;
  }

  private String createTableName() {
    return this.baseName + "-table-" + this.tid++;
  }

  private void createTable(final String tableName, final String... families)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (String family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = new byte[16][];
    byte[] hex = Bytes.toBytes("0123456789abcdef");
    for (int i = 0; i < 16; ++i) {
      splitKeys[i] = new byte[] { hex[i] };
    }
    admin.createTable(htd, splitKeys);
    LOG.debug("Created table=" + tableName);
  }

  private void loadData(final String tableName, int rows, boolean noWAL, String... families)
      throws IOException {
    HTableInterface table = this.tablePool.getTable(tableName);
    try {
      loadData(table, rows, noWAL, families);
    } finally {
      table.close();
    }
  }

  private void loadData(final HTableInterface table, int rows, boolean noWAL, String... families)
      throws IOException {
    LOG.debug("Start loading rows=" + rows + " on table=" + Bytes.toString(table.getTableName()));
    byte[] qualifier = Bytes.toBytes("q");
    table.setAutoFlush(false);
    while (rows-- > 0) {
      byte[] value = Bytes.toBytes(System.currentTimeMillis() * 1000000 + rows);
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      Put put = new Put(key);
      put.setWriteToWAL(!noWAL);
      for (String family: families) {
        put.add(Bytes.toBytes(family), qualifier, value);
      }
      table.put(put);
    }
    table.flushCommits();
    LOG.debug("Finished loading table=" + Bytes.toString(table.getTableName()));
  }


  private TableInfo getTableInfo(final String tableName) throws IOException {
    HTableInterface table = this.tablePool.getTable(tableName);
    try {
      return getTableInfo(table);
    } finally {
      table.close();
    }
  }

  private TableInfo getTableInfo(final HTableInterface table) throws IOException {
    try {
      long rowCount = 0;
      Scan scan = new Scan();
      ResultScanner results = table.getScanner(scan);
      MessageDigest digest = MessageDigest.getInstance("MD5");
      for (Result res: results) {
        digest.update(res.getRow());
        rowCount++;
      }
      results.close();
      String hexDigest = new String(Hex.encodeHex(digest.digest()));
      return new TableInfo(Bytes.toString(table.getTableName()), rowCount, hexDigest);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Error computing MD5 hash", e);
    }
  }

  // bin/hbase org.apache.hadoop.hbase.snapshot.SnapshotLongRunningTest
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(HBaseConfiguration.create(), new SnapshotLongRunningTest(), args);
    System.exit(res);
  }
}