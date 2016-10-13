/**
 *
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
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionSplitState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionSplitStateData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
public class RegionSplitProcedure
    extends StateMachineProcedure<MasterProcedureEnv, RegionSplitState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(RegionSplitProcedure.class);

  private HRegionInfo regionToSplit;
  private HRegionInfo daughterRegionA;
  private HRegionInfo daughterRegionB;
  private ServerName targetServer;
  private boolean hasLock;

  public RegionSplitProcedure(final HRegionInfo regionToSplit, final byte[] splitRow)
      throws IOException {
    checkSplitRow(regionToSplit, splitRow);

    final long rid = getDaughterRegionIdTimestamp(regionToSplit);
    final TableName table = regionToSplit.getTable();

    this.regionToSplit = regionToSplit;
    daughterRegionA = new HRegionInfo(table, regionToSplit.getStartKey(), splitRow, false, rid);
    daughterRegionB = new HRegionInfo(table, splitRow, regionToSplit.getEndKey(), false, rid);
  }

  private static void checkSplitRow(final HRegionInfo regionToSplit, final byte[] splitRow)
      throws IOException {
    if (splitRow == null) {
      throw new DoNotRetryIOException("Split row cannot be null");
    }

    if (Bytes.equals(regionToSplit.getStartKey(), splitRow)) {
      throw new DoNotRetryIOException(
        "Split row is equal to startkey: " + Bytes.toStringBinary(splitRow));
    }

    if (!regionToSplit.containsRow(splitRow)) {
      throw new DoNotRetryIOException(
        "Split row is not inside region key range splitKey:" + Bytes.toStringBinary(splitRow) +
        " region: " + regionToSplit);
    }
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link HRegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final HRegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp.  Can't be less than that of parent else will insert
    // at wrong location in hbase:meta (See HBASE-710).
    if (rid < hri.getRegionId()) {
      LOG.warn("Clock skew; parent regions id is " + hri.getRegionId() +
        " but current time here is " + rid);
      rid = hri.getRegionId() + 1;
    }
    return rid;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final RegionSplitState state)
      throws InterruptedException {
    try {
      switch (state) {
        case REGION_SPLIT_PREPARE:
          prepareSplit(env);
          break;
        case REGION_SPLIT_UNASSIGN_PARENT:
          addChildProcedure(createUnassignProcedures(env, getRegionReplication(env)));
          break;
        case REGION_SPLIT_CREATE_DAUGHTERS:
          createDaughters(env);
          break;
        case REGION_SPLIT_ASSIGN_DAUGHTERS:
          addChildProcedure(createAssignProcedures(env, getRegionReplication(env)));
          break;
        case REGION_SPLIT_FINISH:
          return Flow.NO_MORE_STATE;
      }
    } catch (IOException e) {
      // TODO
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final RegionSplitState state)
      throws IOException {
  }

  private boolean prepareSplit(final MasterProcedureEnv env) {
    //if (regionHasReferenceFiles()) return false;
    return true;
  }

  private UnassignProcedure[] createUnassignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final UnassignProcedure[] procs = new UnassignProcedure[regionReplication];
    for (int i = 0; i < procs.length; ++i) {
      final HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(regionToSplit, i);
      procs[i] = env.getAssignmentManager().createUnassignProcedure(hri, null, true);
    }
    return procs;
  }

  private void createDaughters(final MasterProcedureEnv env) {
    // create reference files
    // update region info of the parent to offline
    // update meta
  }

  private AssignProcedure[] createAssignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final AssignProcedure[] procs = new AssignProcedure[regionReplication * 2];
    for (int i = 0; i < procs.length; ++i) {
      final HRegionInfo daughterHri = (i < regionReplication ? daughterRegionA : daughterRegionB);
      final HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(daughterHri, i);
      procs[i] = env.getAssignmentManager().createAssignProcedure(hri, targetServer);
    }
    return procs;
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
  }

  public HRegionInfo getRegionInfo() {
    return regionToSplit;
  }

  @Override
  public TableName getTableName() {
    return getRegionInfo().getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.SPLIT;
  }

  @Override
  protected RegionSplitState getState(final int stateId) {
    return RegionSplitState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final RegionSplitState state) {
    return state.getNumber();
  }

  @Override
  protected RegionSplitState getInitialState() {
    return RegionSplitState.REGION_SPLIT_PREPARE;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final RegionSplitStateData.Builder state = RegionSplitStateData.newBuilder()
        .setRegionToSplit(HRegionInfo.convert(getRegionInfo()));
    if (daughterRegionA != null) {
      state.setDaughterRegionA(HRegionInfo.convert(daughterRegionA));
    }
    if (daughterRegionB != null) {
      state.setDaughterRegionB(HRegionInfo.convert(daughterRegionB));
    }
    if (targetServer != null) {
      state.setDestinationServer(ProtobufUtil.toServerName(targetServer));
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final RegionSplitStateData state = RegionSplitStateData.parseDelimitedFrom(stream);
    regionToSplit = HRegionInfo.convert(state.getRegionToSplit());
    if (state.hasDaughterRegionA()) {
      daughterRegionA = HRegionInfo.convert(state.getDaughterRegionA());
    }
    if (state.hasDaughterRegionA()) {
      daughterRegionB = HRegionInfo.convert(state.getDaughterRegionB());
    }
    if (state.hasDestinationServer()) {
      targetServer = ProtobufUtil.toServerName(state.getDestinationServer());
    }
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    // unless we are assigning meta, wait for meta to be available and loaded.
    if (env.getAssignmentManager().waitMetaLoaded(this) ||
        env.getAssignmentManager().waitMetaInitialized(this, getRegionInfo())) {
      return LockState.LOCK_EVENT_WAIT;
    }

    // TODO: Revisit this and move it to the executor
    hasLock = !env.getProcedureScheduler().waitRegion(this, getRegionInfo());
    return hasLock ? LockState.LOCK_ACQUIRED : LockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegion(this, getRegionInfo());
    hasLock = false;
  }

  @Override
  protected boolean holdLock(final MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected boolean hasLock(final MasterProcedureEnv env) {
    return hasLock;
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // The operation is triggered internally on the server
    // the client does not know about this procedure.
    return false;
  }
}
