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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionStateData;

@InterfaceAudience.Private
public class MoveRegionProcedure
    extends StateMachineProcedure<MasterProcedureEnv, MoveRegionState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(MoveRegionProcedure.class);

  private RegionPlan plan;

  public MoveRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public MoveRegionProcedure(final RegionPlan plan) {
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final MoveRegionState state)
      throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    switch (state) {
      case MOVE_REGION_UNASSIGN:
        addChildProcedure(new UnassignProcedure(plan.getRegionInfo(), plan.getDestination(), true));
        setNextState(MoveRegionState.MOVE_REGION_ASSIGN);
        break;
      case MOVE_REGION_ASSIGN:
        addChildProcedure(new AssignProcedure(plan.getRegionInfo(), plan.getDestination()));
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final MoveRegionState state)
      throws IOException {
    // no-op
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    return false;
  }

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append("(plan=");
    sb.append(plan);
    sb.append(")");
  }

  @Override
  protected MoveRegionState getInitialState() {
    return MoveRegionState.MOVE_REGION_UNASSIGN;
  }

  @Override
  protected int getStateId(final MoveRegionState state) {
    return state.getNumber();
  }

  @Override
  protected MoveRegionState getState(final int stateId) {
    return MoveRegionState.valueOf(stateId);
  }

  @Override
  public TableName getTableName() {
    return plan.getRegionInfo().getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final MoveRegionStateData.Builder state = MoveRegionStateData.newBuilder()
        .setRegionInfo(HRegionInfo.convert(plan.getRegionInfo()))
        .setSourceServer(ProtobufUtil.toServerName(plan.getSource()))
        .setDestinationServer(ProtobufUtil.toServerName(plan.getDestination()));
    state.build().writeDelimitedTo(stream);
  }

  @Override
  protected void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final MoveRegionStateData state = MoveRegionStateData.parseDelimitedFrom(stream);
    final HRegionInfo regionInfo = HRegionInfo.convert(state.getRegionInfo());
    final ServerName sourceServer = ProtobufUtil.toServerName(state.getSourceServer());
    final ServerName destinationServer = ProtobufUtil.toServerName(state.getDestinationServer());
    this.plan = new RegionPlan(regionInfo, sourceServer, destinationServer);
  }
}
