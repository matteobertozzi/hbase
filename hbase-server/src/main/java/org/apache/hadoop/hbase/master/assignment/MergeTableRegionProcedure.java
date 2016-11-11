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

package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MergeTableRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.annotations.VisibleForTesting;

/**
 * The procedure to merge regions in a table.
 */
@InterfaceAudience.Private
public class MergeTableRegionProcedure
    extends AbstractStateMachineTableProcedure<MergeTableRegionState> {
  private static final Log LOG = LogFactory.getLog(MergeTableRegionProcedure.class);

  private Boolean traceEnabled = null;

  private HRegionInfo regionToMergeA;
  private HRegionInfo regionToMergeB;
  private HRegionInfo mergedRegion;

  public MergeTableRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public MergeTableRegionProcedure(final MasterProcedureEnv env,
      final HRegionInfo regionToMergeA, final HRegionInfo regionToMergeB) throws IOException {
    super(env);

    // TODO
    this.regionToMergeA = regionToMergeA;
    this.regionToMergeB = regionToMergeB;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final MergeTableRegionState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case MERGE_TABLE_REGION_PREPARE:
        if (prepareMergeRegion(env)) {
          setNextState(MergeTableRegionState.MERGE_TABLE_REGION_PRE_OPERATION);
          break;
        } else {
          assert isFailed() : "merge region should have an exception here";
          return Flow.NO_MORE_STATE;
        }
      case MERGE_TABLE_REGION_POST_OPERATION:
        postMergeRegion(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Error trying to merge region " + mergedRegion.getEncodedName() + " in the table "
          + getTableName() + " (in state=" + state + ")";
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retry.
        LOG.warn(msg, e);
      } else {
        LOG.error(msg, e);
        setFailure("master-merge-region", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final MergeTableRegionState state)
      throws IOException, InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

    try {
      switch (state) {
      case MERGE_TABLE_REGION_POST_OPERATION:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      case MERGE_TABLE_REGION_PRE_OPERATION:
        preMergeRegionRollback(env);
        break;
      case MERGE_TABLE_REGION_PREPARE:
        break; // nothing to do
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for mergeting the region "
        + mergedRegion.getEncodedName() + " in table " + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  @Override
  protected boolean isRollbackSupported(final MergeTableRegionState state) {
    switch (state) {
      case MERGE_TABLE_REGION_POST_OPERATION:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
    }
    return true;
  }

  @Override
  protected MergeTableRegionState getState(final int stateId) {
    return MergeTableRegionState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final MergeTableRegionState state) {
    return state.getNumber();
  }

  @Override
  protected MergeTableRegionState getInitialState() {
    return MergeTableRegionState.MERGE_TABLE_REGION_PREPARE;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final MasterProcedureProtos.MergeTableRegionStateData.Builder mergeTableRegionMsg =
        MasterProcedureProtos.MergeTableRegionStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setMergedRegionInfo(HRegionInfo.convert(mergedRegion))
        .addRegionInfoToMerge(HRegionInfo.convert(regionToMergeA))
        .addRegionInfoToMerge(HRegionInfo.convert(regionToMergeB));
    mergeTableRegionMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final MasterProcedureProtos.MergeTableRegionStateData mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(mergeTableRegionsMsg.getUserInfo()));
    mergedRegion = HRegionInfo.convert(mergeTableRegionsMsg.getMergedRegionInfo());
    assert(mergeTableRegionsMsg.getRegionInfoToMergeCount() == 2);
    regionToMergeA = HRegionInfo.convert(mergeTableRegionsMsg.getRegionInfoToMerge(0));
    regionToMergeB = HRegionInfo.convert(mergeTableRegionsMsg.getRegionInfoToMerge(1));
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append("(table=");
    sb.append(getTableName());
    sb.append(" regionA=");
    sb.append(regionToMergeA.getShortNameToLog());
    sb.append(" regionB=");
    sb.append(regionToMergeB.getShortNameToLog());
    sb.append(" mergedRegion=");
    sb.append(mergedRegion.getShortNameToLog());
    sb.append(")");
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) return LockState.LOCK_EVENT_WAIT;

    if (env.getProcedureScheduler().waitRegions(this, getTableName(),
        regionToMergeA, regionToMergeB, mergedRegion)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(),
      regionToMergeA, regionToMergeB, mergedRegion);
  }

  @Override
  public TableName getTableName() {
    return mergedRegion.getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.MERGE;
  }

  /**
   * Prepare to Merge region.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @VisibleForTesting
  public boolean prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is mergeable
    /*
    RegionStateNode node = env.getAssignmentManager().getRegionStates().getRegionNode(parentHRI);
    if (node != null) {
      parentHRI = node.getRegionInfo();

      // expected parent to be online or closed
      if (!node.isInState(State.OPEN, State.CLOSED)) {
        setFailure("master-merge-region",
          new IOException("Merge region " + parentHRI + " failed due to region is not mergeable"));
        return false;
      }

      // lookup the parent HRI state from the AM, which has the latest updated info.
      if (parentHRI.isOffline()) {
        setFailure("master-merge-region",
          new IOException("Merge region " + parentHRI + " failed due to region is not mergeable"));
        return false;
      }
    }*/

    // since we have the lock and the master is coordinating the operation
    // we are always able to merge the region
    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      LOG.warn("merge switch is off! skip merge of " + mergedRegion);
      setFailure("master-merge-region",
          new IOException("Merge region " + mergedRegion + " failed due to merge switch off"));
      return false;
    }
    return true;
  }

  /**
   * Action before mergeting region in a table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preMergeRegion(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      //cpHost.preMergeRegionAction(getTableName(), getUser());
    }
  }

  /**
   * Action during rollback a pre merge table region.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   */
  private void preMergeRegionRollback(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      //cpHost.preRollBackMergeAction(getUser());
    }
  }

  /**
   * Post merge region actions
   * @param env MasterProcedureEnv
   **/
  private void postMergeRegion(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      //cpHost.postCompletedMergeRegionAction(daughter_1_HRI, daughter_2_HRI, getUser());
    }
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }
}
