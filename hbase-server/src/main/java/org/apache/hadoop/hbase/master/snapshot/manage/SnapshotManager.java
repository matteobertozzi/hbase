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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.CloneSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.RestoreSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.TableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
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
public class SnapshotManager {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  public class OperationStatus {
    private Throwable error = null;
    private boolean done = false;

    public OperationStatus() {
    }

    public void completed() {
      done = true;
    }

    public void failed(final Throwable e) {
      done = true;
      error = e;
    }

    public boolean isDone() {
      return done;
    }

    public boolean isFailed() {
      return error != null;
    }

    public Throwable getError() {
      return error;
    }
  }

  // TODO - enable having multiple snapshots with multiple monitors

  private final MasterServices master;
  private SnapshotErrorMonitorFactory errorFactory;
  private final ExceptionOrchestrator<HBaseSnapshotException> dispatcher;
  private TableSnapshotHandler snapshotHandler;
  private volatile boolean snapshotAborted;

  private final Map<SnapshotDescription, OperationStatus> pendingRestores =
    new HashMap<SnapshotDescription, OperationStatus>();

  public SnapshotManager(final MasterServices master, final ZooKeeperWatcher watcher)
      throws KeeperException {
    this.master = master;
    this.errorFactory = new SnapshotErrorMonitorFactory();
    this.dispatcher = errorFactory.getHub();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot in process, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  public boolean isSnapshotInProcess() throws SnapshotCreationException {
    return snapshotHandler != null && !snapshotHandler.getFinished();
  }

  public boolean isSnapshotAborted() {
    return this.snapshotAborted;
  }

  public void abortSnapshot(String why, Throwable e) {
    // short circuit
    if (this.isSnapshotAborted()) return;

    this.snapshotAborted = true;
    LOG.warn("Aborting because: " + why, e);

    // pass the abort onto all the listeners
    this.dispatcher.receiveError(why, new HBaseSnapshotException(e));
  }

  public synchronized DisabledTableSnapshotHandler newDisabledTableSnapshotHandler(
      SnapshotDescription snapshot, Server parent) throws IOException {
    // Reset the snapshot to be the disabled snapshot
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();
    // create a monitor for snapshot errors
    SnapshotErrorListener monitor = this.errorFactory.createMonitorForSnapshot(snapshot);
    DisabledTableSnapshotHandler handler = new DisabledTableSnapshotHandler(snapshot, parent,
        this.master, monitor, this);
    this.snapshotHandler = handler;
    return handler;
  }

  /**
   * @return SnapshotTracker for current snapshot
   */
  public TableSnapshotHandler getCurrentSnapshotMonitor() {
    return this.snapshotHandler;
  }

  /**
   * Reset the manager to allow another snapshot to precede
   * @param snapshotDir final path of the snapshot
   * @param workingDir directory where the in progress snapshot was built
   * @param fs {@link FileSystem} where the snapshot was built
   * @throws SnapshotCreationException if the snapshot could not be moved
   * @throws IOException the filesystem could not be reached
   */
  public void completeSnapshot(Path snapshotDir, Path workingDir, FileSystem fs)
      throws SnapshotCreationException, IOException {
    if (this.snapshotHandler == null) return;
    LOG.debug("Setting handler to finsihed.");
    this.snapshotHandler.finish();
    LOG.debug("Sentinel is done, just moving the snapshot from " + workingDir + " to "
        + snapshotDir);
    if (!fs.rename(workingDir, snapshotDir)) {
      throw new SnapshotCreationException("Failed to move working directory(" + workingDir
          + ") to completed directory(" + snapshotDir + ").");
    }
  }

  /**
   * Reset the state of the manager.
   * <p>
   * Exposed for TESTING.
   */
  public void resetSnaphot() {
    setSnapshotHandler(null);
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   */
  public void setSnapshotHandler(TableSnapshotHandler handler) {
    this.snapshotHandler = handler;
  }

  /**
   * EXPOSED FOR TESTING.
   * @return the {@link ExceptionOrchestrator} that updates all running {@link TableSnapshotHandler}
   *         in the even of a n abort.
   */
  ExceptionOrchestrator<HBaseSnapshotException> getExceptionOrchestrator() {
    return this.dispatcher;
  }

  /**
   * Create a new restore snapshot handler
   */
  public RestoreSnapshotHandler newRestoreSnapshotHandler(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws IOException {
    RestoreSnapshotHandler handler = new RestoreSnapshotHandler(this.master, this,
      snapshot, hTableDescriptor, waitTime);
    this.pendingRestores.put(snapshot, new OperationStatus());
    return handler;
  }

  /**
   * Create a new clone from snapshot handler
   */
  public CloneSnapshotHandler newCloneSnapshotHandler(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws IOException {
    CloneSnapshotHandler handler = new CloneSnapshotHandler(this.master, this,
      snapshot, hTableDescriptor, waitTime);
    this.pendingRestores.put(snapshot, new OperationStatus());
    return handler;
  }

  /**
   * @return True if the specified snapshot is in progress
   */
  public boolean isRestoreInProcess(final SnapshotDescription snapshot) {
    OperationStatus status = pendingRestores.get(snapshot);
    return status != null && !status.isDone();
  }

  /**
   * Remove the snapshot from the pending list. Operation done by the master
   * when the client ask if the restore is finished and the restore operation is done.
   * @return the restore operation status
   */
  public OperationStatus restoreRemove(final SnapshotDescription snapshot) {
    return this.pendingRestores.remove(snapshot);
  }

  /**
   * Called by the restore/clone handler to notify that the restore is completed
   */
  public void restoreCompleted(final SnapshotDescription snapshot) {
    OperationStatus status = this.pendingRestores.get(snapshot);
    if (status != null) status.completed();
  }

  /**
   * Called by the restore/clone handler to notify that the restore is failed
   */
  public void restoreFailed(final SnapshotDescription snapshot, final Throwable e) {
    OperationStatus status = this.pendingRestores.get(snapshot);
    if (status != null) status.failed(e);
  }
}