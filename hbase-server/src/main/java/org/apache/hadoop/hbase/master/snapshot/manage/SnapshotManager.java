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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.CloneSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.RestoreSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class monitors the whole process of snapshots via ZooKeeper. There is only one
 * SnapshotMonitor for the master.
 * <p>
 * Start monitoring a snapshot by calling method monitor() before the snapshot is started across the
 * cluster via ZooKeeper. SnapshotMonitor would stop monitoring this snapshot only if it is finished
 * or aborted.
 * <p>
 * Note: There could be only one snapshot being processed and monitored at a time over the cluster.
 * Start monitoring a snapshot only when the previous one reaches an end status.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SnapshotManager implements Stoppable {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  // TODO - enable having multiple snapshots with multiple monitors

  // Restore Handlers map, with table name as key
  private Map<String, SnapshotHandler> restoreHandlers = new HashMap<String, SnapshotHandler>();

  private final MasterServices master;
  private SnapshotHandler handler;
  private ExecutorService pool;
  private final Path rootDir;

  private boolean stopped;

  public SnapshotManager(final MasterServices master, final ZooKeeperWatcher watcher,
      final ExecutorService executorService) throws KeeperException {
    this.master = master;
    this.pool = executorService;
    this.rootDir = master.getMasterFileSystem().getRootDir();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot currently being taken, <tt>false</tt> otherwise
   */
  public boolean isTakingSnapshot() {
    return handler != null && !handler.isFinished();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot in progress of the specified table.
   */
  public boolean isTakingSnapshot(final String tableName) {
    return isTakingSnapshot() && handler.getSnapshot().getTable().equals(tableName);
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot or restore operation on the target table.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  private synchronized void prepareToTakeSnapshot(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);

    // make sure we aren't already running a snapshot
    if (isTakingSnapshot()) {
      throw new SnapshotCreationException("Already running another snapshot:"
          + this.handler.getSnapshot(), snapshot);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(snapshot.getTable())) {
      throw new SnapshotCreationException("Restore in progress on the same table snapshot:"
          + this.handler.getSnapshot(), snapshot);
    }

    try {
      // delete the woring directory, since we aren't running the snapshot
      fs.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!fs.mkdirs(workingDir)) {
        throw new SnapshotCreationException("Couldn't create working directory (" + workingDir
            + ") for snapshot.", snapshot);
      }
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
          "Exception while checking to see if snapshot could be started.", e, snapshot);
    }
  }

  /**
   * Take a snapshot of a disabled table.
   * <p>
   * Ensures the snapshot won't be started if there is another snapshot already running. Does
   * <b>not</b> check to see if another snapshot of the same name already exists.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @param parent server where the snapshot is being run
   * @throws HBaseSnapshotException if the snapshot could not be started
   */
  public synchronized void snapshotDisabledTable(SnapshotDescription snapshot, Server parent)
      throws HBaseSnapshotException {
    // setup the snapshot
    prepareToTakeSnapshot(snapshot);

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    DisabledTableSnapshotHandler handler;
    try {
      handler = new DisabledTableSnapshotHandler(snapshot, parent, this.master);
      this.handler = handler;
      this.pool.submit(handler);
    } catch (IOException e) {
      // cleanup the working directory
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
      try {
        if (this.master.getMasterFileSystem().getFileSystem().delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:"
              + snapshot);
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" + snapshot);
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e, snapshot);
    }
  }

  /**
   * @return the current handler for the snapshot
   */
  public SnapshotHandler getCurrentSnapshotHandler() {
    return this.handler;
  }

  /**
   * Restore the specified snapshot.
   * The restore will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor of the table to create
   * @param waitTime timeout before considering the clone failed
   */
  public synchronized void cloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws HBaseSnapshotException {
    String tableName = hTableDescriptor.getNameAsString();

    // make sure we aren't running a snapshot on the same table
    if (isTakingSnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      CloneSnapshotHandler handler =
        new CloneSnapshotHandler(master, snapshot, hTableDescriptor, waitTime);
      this.pool.submit(handler);
      restoreHandlers.put(tableName, handler);
    } catch (Exception e) {
      String msg = "Couldn't clone the snapshot=" + snapshot + " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Restore the specified snapshot.
   * The restore will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor
   * @param waitTime timeout before considering the restore failed
   */
  public synchronized void restoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws HBaseSnapshotException {
    String tableName = hTableDescriptor.getNameAsString();

    // make sure we aren't running a snapshot on the same table
    if (isTakingSnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      RestoreSnapshotHandler handler =
        new RestoreSnapshotHandler(master, snapshot, hTableDescriptor, waitTime);
      this.pool.submit(handler);
      restoreHandlers.put(hTableDescriptor.getNameAsString(), handler);
    } catch (Exception e) {
      String msg = "Couldn't restore the snapshot=" + snapshot + " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Verify if the the restore of the specified table is in progress.
   *
   * @param tableName table under restore
   * @return <tt>true</tt> if there is a restore in progress of the specified table.
   */
  public boolean isRestoringTable(final String tableName) {
    SnapshotHandler sentinel = restoreHandlers.get(tableName);
    return(sentinel != null && !sentinel.isFinished());
  }

  /**
   * Get the restore snapshot handler for the specified table
   * @param tableName table under restore
   * @return the restore snapshot handler
   */
  public SnapshotHandler getRestoreSnapshotHandler(final String tableName) {
    return restoreHandlers.get(tableName);
  }

  @Override
  public void stop(String why) {
    // short circuit
    if (this.stopped) return;
    // make sure we get stop
    this.stopped = true;
    // pass the stop onto all the listeners
    if (this.handler != null) this.handler.stop(why);
    // pass the stop onto all the restore handlers
    for (SnapshotHandler restoreHandler: this.restoreHandlers.values()) {
      restoreHandler.stop(why);
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   */
  public void setSnapshotHandlerForTesting(SnapshotHandler handler) {
    this.handler = handler;
  }
}
