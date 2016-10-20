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

package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.AssignmentListener;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDataManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TakeSnapshotState;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

@InterfaceAudience.Private
public class TakeSnapshotProcedure
    extends AbstractStateMachineTableProcedure<TakeSnapshotState>
    implements AssignmentListener {
  private static final Log LOG = LogFactory.getLog(TakeSnapshotProcedure.class);

  private SnapshotDescription snapshot;
  private TableName tableName;

  public final class SnapshotRegionState {
    private ServerName serverName;
    private SnapshotRegionManifest manifest;

    public ServerName getServerName() {
      return serverName;
    }

    public void setServerName(final ServerName serverName) {
      this.serverName = serverName;
    }

    public SnapshotRegionManifest getRegionManifests() {
      return manifest;
    }

    public void setRegionManifest(final SnapshotRegionManifest manifest) {
      this.manifest = manifest;
    }
  }

  private Map<HRegionInfo, SnapshotRegionState> regions =
      new ConcurrentHashMap<HRegionInfo, SnapshotRegionState>();

  // Monitor
  private MonitoredTask monitorStatus = null;

  public TakeSnapshotProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public TakeSnapshotProcedure(final MasterProcedureEnv env, final SnapshotDescription snapshot) {
    super(env);
    // Snapshot information
    this.snapshot = snapshot;
    this.tableName = TableName.valueOf(snapshot.getTable());

    // Monitor
    getMonitorStatus();
  }

  /**
   * Set up monitor status if it is not created.
   */
  private MonitoredTask getMonitorStatus() {
    if (monitorStatus == null) {
      monitorStatus = TaskMonitor.get().createStatus(
        "Taking  snapshot '" + snapshot.getName() + "' of table " + getTableName());
    }
    return monitorStatus;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final TakeSnapshotState state)
      throws InterruptedException {
    // Make sure that the monitor status is set up
    getMonitorStatus();
    addRegionListener(env);

    if (!hasTimeout()) {
      final Configuration conf = env.getMasterConfiguration();
      final int timeoutMillis = Math.max(
        conf.getInt(SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_KEY,
                     SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT),
        conf.getInt(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
                     SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME));
      setTimeout(timeoutMillis);
    }

    try {
      switch (state) {
        case TAKE_SNAPSHOT_PRE_OPERATION:
          throw new IOException("TODO: Implement me!");
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      setFailure("master-take-snapshot", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final TakeSnapshotState state)
      throws IOException {
    // TODO: drop folders!
  }

  public void addRegionListener(final MasterProcedureEnv env) {
    env.getAssignmentManager().registerListener(this);
  }

  @Override
  public void regionOpened(final HRegionInfo regionInfo, final ServerName serverName) {
    SnapshotRegionState state = regions.get(regionInfo);
    if (state == null) {
      state = new SnapshotRegionState();
      regions.put(regionInfo, state);
    }
    state.setServerName(serverName);
  }

  @Override
  public void regionClosed(final HRegionInfo regionInfo) {
    final SnapshotRegionState state = regions.get(regionInfo);
    if (state != null) state.setServerName(null);
  }

  @Override
  public void regionSplitted(final HRegionInfo parent, final HRegionInfo hriA,
      final HRegionInfo hriB) {
    regions.remove(parent);
    regions.put(hriA, new SnapshotRegionState());
    regions.put(hriB, new SnapshotRegionState());
  }

  @Override
  public void regionsMerged(final HRegionInfo hriA, final HRegionInfo hriB,
      final HRegionInfo merged) {
    regions.remove(hriA);
    regions.remove(hriB);
    regions.put(merged, new SnapshotRegionState());
  }

  @Override
  protected boolean isRollbackSupported(final TakeSnapshotState state) {
    // always happy to rollback a snapshot
    return true;
  }

  @Override
  protected TakeSnapshotState getState(final int stateId) {
    return TakeSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final TakeSnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected TakeSnapshotState getInitialState() {
    return TakeSnapshotState.TAKE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.READ; // Restore is modifying a table
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    // TODO: We may be able to abort if the procedure is not started yet.
    return false;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append("(table=");
    sb.append(getTableName());
    sb.append(" snapshot=");
    sb.append(snapshot);
    sb.append(")");
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final MasterProcedureProtos.TakeSnapshotStateData.Builder state =
      MasterProcedureProtos.TakeSnapshotStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setSnapshot(this.snapshot);
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final MasterProcedureProtos.TakeSnapshotStateData state =
      MasterProcedureProtos.TakeSnapshotStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    snapshot = state.getSnapshot();
    tableName = TableName.valueOf(snapshot.getTable());
  }
}
