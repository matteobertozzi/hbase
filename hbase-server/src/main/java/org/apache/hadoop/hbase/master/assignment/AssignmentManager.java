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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.RegionStateListener;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.AssignmentListener;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates.ServerState;
import org.apache.hadoop.hbase.master.assignment.RegionStates.ServerStateNode;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureInMemoryChore;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

// TODO: why are they here?
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;

/**
 * The AssignmentManager is the coordinator for region assign/unassign operations.
 *  - In-memory states of regions and servers are stored in RegionStates.
 *  - hbase:meta states updates are handled by RegionStateStore.
 *
 * Regions are created by CreateTable, Split, Merge
 * Regions are deleted by DeleteTable, Split, Merge
 * Assigns are triggered by CreateTable, EnableTable, Split, Merge, ServerCrash
 * Unassigns are triggered by DisableTable, Split, Merge
 */
@InterfaceAudience.Private
public class AssignmentManager implements ServerListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  // TODO
  //  - handle region migration
  //  - handle sys table assignment first (e.g. acl, namespace)
  //  - handle table priorities

  public static final String BOOTSTRAP_THREAD_POOL_SIZE_CONF_KEY =
      "hbase.assignment.bootstrap.thread.pool.size";
  private static final int DEFAULT_BOOTSTRAP_THREAD_POOL_SIZE = 16;

  public static final String ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY =
      "hbase.assignment.dispatch.wait.msec";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC = 150;

  public static final String ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY =
      "hbase.assignment.dispatch.wait.queue.max.size";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX = 100;

  public static final String RIT_CHORE_INTERVAL_MSEC_CONF_KEY =
      "hbase.assignment.rit.chore.interval.msec";
  private static final int DEFAULT_RIT_CHORE_INTERVAL_MSEC = 5 * 1000;

  public static final String ASSIGN_MAX_ATTEMPTS =
      "hbase.assignment.maximum.attempts";
  private static final int DEFAULT_ASSIGN_MAX_ATTEMPTS = 10;

  /** Region in Transition metrics threshold time */
  public static final String METRICS_RIT_STUCK_WARNING_THRESHOLD =
      "hbase.metrics.rit.stuck.warning.threshold";
  private static final int DEFAULT_RIT_STUCK_WARNING_THRESHOLD = 60 * 1000;

  private final ProcedureEvent metaInitializedEvent = new ProcedureEvent("meta initialized");
  private final ProcedureEvent metaLoadEvent = new ProcedureEvent("meta load");

  /**
   * Indicator that AssignmentManager has recovered the region states so
   * that ServerCrashProcedure can be fully enabled and re-assign regions
   * of dead servers. So that when re-assignment happens, AssignmentManager
   * has proper region states.
   */
  private final ProcedureEvent failoverCleanupDone = new ProcedureEvent("failover cleanup");

  /** Listeners that are called on assignment events. */
  private final CopyOnWriteArrayList<AssignmentListener> listeners =
      new CopyOnWriteArrayList<AssignmentListener>();

  // TODO: why is this different from the listeners (carried over from the old AM)
  private RegionStateListener regionStateListener;

  private final MetricsAssignmentManager metrics;
  private final RegionInTransitionChore ritChore;
  private final MasterServices master;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final RegionStates regionStateMap = new RegionStates();
  private final RegionStateStore regionStateStore;

  private final boolean shouldAssignRegionsWithFavoredNodes;
  private final int assignDispatchWaitQueueMaxSize;
  private final int assignDispatchWaitMillis;
  private final int assignMaxAttempts;

  private Thread assignThread;

  public AssignmentManager(final MasterServices master) {
    this(master, new RegionStateStore(master));
  }

  public AssignmentManager(final MasterServices master, final RegionStateStore stateStore) {
    this.master = master;
    this.regionStateStore = stateStore;
    this.metrics = new MetricsAssignmentManager();

    final Configuration conf = master.getConfiguration();

    // Only read favored nodes if using the favored nodes load balancer.
    this.shouldAssignRegionsWithFavoredNodes = FavoredNodeLoadBalancer.class.isAssignableFrom(
        conf.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, Object.class));

    this.assignDispatchWaitMillis = conf.getInt(ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY,
        DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC);
    this.assignDispatchWaitQueueMaxSize = conf.getInt(ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY,
        DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX);

    this.assignMaxAttempts = Math.max(1, conf.getInt(ASSIGN_MAX_ATTEMPTS,
        DEFAULT_ASSIGN_MAX_ATTEMPTS));

    int ritChoreInterval = conf.getInt(RIT_CHORE_INTERVAL_MSEC_CONF_KEY,
        DEFAULT_RIT_CHORE_INTERVAL_MSEC);
    this.ritChore = new RegionInTransitionChore(ritChoreInterval);
  }

  public void start() throws IOException {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    LOG.info("starting assignment manager");

    // Register Server Listener
    master.getServerManager().registerListener(this);

    // Start the RegionStateStore
    regionStateStore.start();

    // Start the Assignment Thread
    startAssignmentThread();
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    LOG.info("stopping assignment manager");
    final boolean hasProcExecutor = master.getMasterProcedureExecutor() != null;

    // Remove the RIT chore
    if (hasProcExecutor) {
      master.getMasterProcedureExecutor().removeChore(this.ritChore);
    }

    // Stop the Assignment Thread
    stopAssignmentThread();

    // Stop the RegionStateStore
    regionStateMap.clear();
    regionStateStore.stop();

    // Unregister Server Listener
    master.getServerManager().unregisterListener(this);

    // Update meta events (for testing)
    if (hasProcExecutor) {
      getProcedureScheduler().suspendEvent(metaLoadEvent);
      setFailoverCleanupDone(false);
      for (HRegionInfo hri: getMetaRegionSet()) {
        setMetaInitialized(hri, false);
      }
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  public Configuration getConfiguration() {
    return master.getConfiguration();
  }

  public MetricsAssignmentManager getAssignmentManagerMetrics() {
    return metrics;
  }

  private LoadBalancer getBalancer() {
    return master.getLoadBalancer();
  }

  private MasterProcedureEnv getProcedureEnvironment() {
    return master.getMasterProcedureExecutor().getEnvironment();
  }

  private MasterProcedureScheduler getProcedureScheduler() {
    return getProcedureEnvironment().getProcedureScheduler();
  }

  protected int getAssignMaxAttempts() {
    return assignMaxAttempts;
  }

  /**
   * Add the listener to the notification list.
   * @param listener The AssignmentListener to register
   */
  public void registerListener(final AssignmentListener listener) {
    this.listeners.add(listener);
  }

  /**
   * Remove the listener from the notification list.
   * @param listener The AssignmentListener to unregister
   */
  public boolean unregisterListener(final AssignmentListener listener) {
    return this.listeners.remove(listener);
  }

  public void setRegionStateListener(final RegionStateListener listener) {
    this.regionStateListener = listener;
  }

  public RegionStates getRegionStates() {
    return regionStateMap;
  }

  public RegionStateStore getRegionStateStore() {
    return regionStateStore;
  }

  public List<ServerName> getFavoredNodes(final HRegionInfo regionInfo) {
    if (shouldAssignRegionsWithFavoredNodes) {
      return ((FavoredNodeLoadBalancer)getBalancer()).getFavoredNodes(regionInfo);
    }
    return ServerName.EMPTY_SERVER_LIST;
  }

  // ============================================================================================
  //  Table State Manager helpers
  // ============================================================================================
  public TableStateManager getTableStateManager() {
    return master.getTableStateManager();
  }

  public boolean isTableEnabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName, TableState.State.ENABLED);
  }

  public boolean isTableDisabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName,
      TableState.State.DISABLED, TableState.State.DISABLING);
  }

  // ============================================================================================
  //  META Helpers
  // ============================================================================================
  private boolean isMetaRegion(final HRegionInfo regionInfo) {
    return regionInfo.isMetaRegion();
  }

  public boolean isMetaRegion(final byte[] regionName) {
    return getMetaRegionFromName(regionName) != null;
  }

  public HRegionInfo getMetaRegionFromName(final byte[] regionName) {
    for (HRegionInfo hri: getMetaRegionSet()) {
      if (Bytes.equals(hri.getRegionName(), regionName)) {
        return hri;
      }
    }
    return null;
  }

  public boolean isCarryingMeta(final ServerName serverName) {
    for (HRegionInfo hri: getMetaRegionSet()) {
      if (isCarryingMeta(serverName, hri)) {
        return true;
      }
    }
    return false;
  }

  public boolean isCarryingMeta(final ServerName serverName, final HRegionInfo regionInfo) {
    return isCarryingRegion(serverName, regionInfo);
  }

  public boolean isCarryingMetaReplica(final ServerName serverName, final HRegionInfo regionInfo) {
    return isCarryingRegion(serverName, regionInfo);
  }

  private boolean isCarryingRegion(final ServerName serverName, final HRegionInfo regionInfo) {
    // TODO: check for state?
    final RegionStateNode node = regionStateMap.getRegionNode(regionInfo);
    return(node != null && serverName.equals(node.getRegionLocation()));
  }

  private HRegionInfo getMetaForRegion(final HRegionInfo regionInfo) {
    //if (regionInfo.isMetaRegion()) return regionInfo;
    // TODO: handle multiple meta. if the region provided is not meta lookup
    // which meta the region belongs to.
    return HRegionInfo.FIRST_META_REGIONINFO;
  }

  // TODO: handle multiple meta.
  private static final Set<HRegionInfo> META_REGION_SET =
      Collections.singleton(HRegionInfo.FIRST_META_REGIONINFO);
  public Set<HRegionInfo> getMetaRegionSet() {
    return META_REGION_SET;
  }

  // ============================================================================================
  //  META Event(s) helpers
  // ============================================================================================
  public boolean isMetaInitialized() {
    return metaInitializedEvent.isReady();
  }

  public boolean isMetaRegionInTransition() {
    return !isMetaInitialized();
  }

  public boolean waitMetaInitialized(final Procedure proc) {
    // TODO: handle multiple meta. should this wait on all meta?
    // this is used by the ServerCrashProcedure...
    return waitMetaInitialized(proc, HRegionInfo.FIRST_META_REGIONINFO);
  }

  public boolean waitMetaInitialized(final Procedure proc, final HRegionInfo regionInfo) {
    return getProcedureScheduler().waitEvent(
      getMetaInitializedEvent(getMetaForRegion(regionInfo)), proc);
  }

  private void setMetaInitialized(final HRegionInfo metaRegionInfo, final boolean isInitialized) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    final ProcedureEvent metaInitEvent = getMetaInitializedEvent(metaRegionInfo);
    if (isInitialized) {
      getProcedureScheduler().wakeEvent(metaInitEvent);
    } else {
      getProcedureScheduler().suspendEvent(metaInitEvent);
    }
  }

  private ProcedureEvent getMetaInitializedEvent(final HRegionInfo metaRegionInfo) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    // TODO: handle multiple meta.
    return metaInitializedEvent;
  }

  public boolean waitMetaLoaded(final Procedure proc) {
    return getProcedureScheduler().waitEvent(metaLoadEvent, proc);
  }

  protected void wakeMetaLoadedEvent() {
    getProcedureScheduler().wakeEvent(metaLoadEvent);
    assert isMetaLoaded() : "expected meta to be loaded";
  }

  public boolean isMetaLoaded() {
    return metaLoadEvent.isReady();
  }

  // ============================================================================================
  //  TODO: Sync helpers
  // ============================================================================================
  public void assignMeta(final HRegionInfo metaRegionInfo) throws IOException {
    assignMeta(metaRegionInfo, null);
  }

  public void assignMeta(final HRegionInfo metaRegionInfo, final ServerName serverName)
      throws IOException {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    AssignProcedure proc;
    if (serverName != null) {
      LOG.debug("try assigning Meta " + metaRegionInfo + " to " + serverName);
      proc = createAssignProcedure(metaRegionInfo, serverName);
    } else {
      LOG.debug("assigning Meta " + metaRegionInfo);
      proc = createAssignProcedure(metaRegionInfo, false);
    }
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public void assign(final HRegionInfo regionInfo) throws IOException {
    assign(regionInfo, true);
  }

  public void assign(final HRegionInfo regionInfo, final boolean forceNewPlan) throws IOException {
    AssignProcedure proc = createAssignProcedure(regionInfo, forceNewPlan);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public void unassign(final HRegionInfo regionInfo) {
    // TODO: rename this in reassign
  }

  @VisibleForTesting
  public void reopen(final HRegionInfo region) {
    // TODO: used by TestScannersFromClientSide.java???
  }

  public Future<byte[]> moveAsync(final RegionPlan regionPlan) {
    MoveRegionProcedure proc = createMoveRegionProcedure(regionPlan);
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public boolean waitForAssignment(final HRegionInfo regionInfo) throws IOException {
    return waitForAssignment(regionInfo, Long.MAX_VALUE);
  }

  public boolean waitForAssignment(final HRegionInfo regionInfo, final long timeout) throws IOException {
    RegionStateNode node = regionStateMap.getRegionNode(regionInfo);
    if (node == null) return false;

    RegionTransitionProcedure proc = node.getProcedure();
    if (proc == null) return false;

    ProcedureSyncWait.waitForProcedureToCompleteIOE(
      master.getMasterProcedureExecutor(), proc.getProcId(), timeout);
    return true;
  }

  // ============================================================================================
  //  RegionTransition procedures helpers
  // ============================================================================================
  public AssignProcedure[] createAssignProcedures(final Collection<HRegionInfo> regionInfo) {
    return createAssignProcedures(regionInfo, false);
  }

  public AssignProcedure[] createAssignProcedures(final Collection<HRegionInfo> regionInfo,
      final boolean forceNewPlan) {
    final AssignProcedure[] procs = new AssignProcedure[regionInfo.size()];
    int index = 0;
    for (HRegionInfo hri: regionInfo) {
      procs[index++] = createAssignProcedure(hri, forceNewPlan);
    }
    return procs;
  }

  public UnassignProcedure[] createUnassignProcedures(final Collection<HRegionInfo> regionInfo) {
    final UnassignProcedure[] procs = new UnassignProcedure[regionInfo.size()];
    int index = 0;
    for (HRegionInfo hri: regionInfo) {
      procs[index++] = createUnassignProcedure(hri, null, false);
    }
    return procs;
  }

  public MoveRegionProcedure[] createReopenProcedures(final Collection<HRegionInfo> regionInfo) {
    final MoveRegionProcedure[] procs = new MoveRegionProcedure[regionInfo.size()];
    int index = 0;
    for (HRegionInfo hri: regionInfo) {
      final ServerName serverName = regionStateMap.getRegionServerOfRegion(hri);
      final RegionPlan plan = new RegionPlan(hri, serverName, serverName);
      procs[index++] = createMoveRegionProcedure(plan);
    }
    return procs;
  }

  /**
   * Called by things like EnableTableProcedure to get a list of AssignProcedure
   * to assign the regions of the table.
   */
  public AssignProcedure[] createAssignProcedures(final TableName tableName) {
    return createAssignProcedures(regionStateMap.getRegionsOfTable(tableName));
  }

  /**
   * Called by things like DisableTableProcedure to get a list of UnassignProcedure
   * to unassign the regions of the table.
   */
  public UnassignProcedure[] createUnassignProcedures(final TableName tableName) {
    return createUnassignProcedures(regionStateMap.getRegionsOfTable(tableName));
  }

  /**
   * Called by things like ModifyColumnFamilyProcedure to get a list of MoveRegionProcedure
   * to reopen the regions of the table.
   */
  public MoveRegionProcedure[] createReopenProcedures(final TableName tableName) {
    return createReopenProcedures(regionStateMap.getRegionsOfTable(tableName));
  }

  public AssignProcedure createAssignProcedure(final HRegionInfo regionInfo,
      final boolean forceNewPlan) {
    AssignProcedure proc = new AssignProcedure(regionInfo, forceNewPlan);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  public AssignProcedure createAssignProcedure(final HRegionInfo regionInfo,
      final ServerName targetServer) {
    AssignProcedure proc = new AssignProcedure(regionInfo, targetServer);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  public UnassignProcedure createUnassignProcedure(final HRegionInfo regionInfo,
      final ServerName destinationServer, final boolean force) {
    UnassignProcedure proc = new UnassignProcedure(regionInfo, destinationServer, force);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  public MoveRegionProcedure createMoveRegionProcedure(final RegionPlan plan) {
    MoveRegionProcedure proc = new MoveRegionProcedure(plan);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  public SplitTableRegionProcedure createSplitProcedure(final HRegionInfo regionToSplit,
      final byte[] splitKey) throws IOException {
    return new SplitTableRegionProcedure(getProcedureEnvironment(), regionToSplit, splitKey);
  }

  /**
   * Delete the region states. This is called by "DeleteTable"
   */
  public void deleteTable(final TableName tableName) throws IOException {
    final ArrayList<HRegionInfo> regions = regionStateMap.getTableRegionsInfo(tableName);
    regionStateStore.deleteRegions(regions);
    for (int i = 0; i < regions.size(); ++i) {
      final HRegionInfo regionInfo = regions.get(i);
      // we expect the region to be offline
      regionStateMap.removeFromOfflineRegions(regionInfo);
      regionStateMap.deleteRegion(regionInfo);
    }
  }

  // ============================================================================================
  //  RS Region Transition Report helpers
  // ============================================================================================
  // TODO: Move this code in MasterRpcServices and call on specific event?
  public ReportRegionStateTransitionResponse reportRegionStateTransition(
      final ReportRegionStateTransitionRequest req) throws PleaseHoldException {
    final ReportRegionStateTransitionResponse.Builder builder =
        ReportRegionStateTransitionResponse.newBuilder();
    final long startTime = EnvironmentEdgeManager.currentTime();
    final ServerName serverName = ProtobufUtil.toServerName(req.getServer());
    try {
      for (RegionStateTransition transition: req.getTransitionList()) {
        switch (transition.getTransitionCode()) {
          case OPENED:
          case FAILED_OPEN:
          case CLOSED:
            assert transition.getRegionInfoCount() == 1 : transition;
            final HRegionInfo hri = HRegionInfo.convert(transition.getRegionInfo(0));
            updateRegionTransition(serverName, transition.getTransitionCode(), hri,
                transition.hasOpenSeqNum() ? transition.getOpenSeqNum() : HConstants.NO_SEQNUM);
            break;
          case READY_TO_SPLIT:
          case SPLIT_PONR:
          case SPLIT:
          case SPLIT_REVERTED:
            assert transition.getRegionInfoCount() == 3 : transition;
            final HRegionInfo parent = HRegionInfo.convert(transition.getRegionInfo(0));
            final HRegionInfo splitA = HRegionInfo.convert(transition.getRegionInfo(1));
            final HRegionInfo splitB = HRegionInfo.convert(transition.getRegionInfo(2));
            updateRegionSplitTransition(serverName, transition.getTransitionCode(),
              parent, splitA, splitB);
            break;
          case READY_TO_MERGE:
          case MERGE_PONR:
          case MERGED:
          case MERGE_REVERTED:
            assert transition.getRegionInfoCount() == 3 : transition;
            final HRegionInfo merged = HRegionInfo.convert(transition.getRegionInfo(0));
            final HRegionInfo mergeA = HRegionInfo.convert(transition.getRegionInfo(1));
            final HRegionInfo mergeB = HRegionInfo.convert(transition.getRegionInfo(2));
            updateRegionMergeTransition(serverName, transition.getTransitionCode(),
              merged, mergeA, mergeB);
            break;
        }
      }
    } catch (UnsupportedOperationException|IOException e) {
      // TODO: at the moment we have a single error message and the RS will abort
      // if the master says that one of the region transition failed.
      LOG.warn("failed to transition: " + e.getMessage());
      builder.setErrorMessage("failed to transition: " + e.getMessage());
    }
    metrics.updateTransitionReportTime(EnvironmentEdgeManager.currentTime() - startTime);
    return builder.build();
  }

  private void updateRegionTransition(final ServerName serverName, final TransitionCode state,
      final HRegionInfo regionInfo, final long seqId)
      throws PleaseHoldException, UnexpectedStateException {
    checkFailoverCleanupCompleted(regionInfo);

    final RegionStateNode regionNode = regionStateMap.getRegionNode(regionInfo);
    if (regionNode == null) {
      // the table/region is gone. maybe a delete, split, merge
      throw new UnexpectedStateException(String.format(
        "Server %s was trying to transition region %s to %s. but the region was removed.",
        serverName, regionInfo, state));
    }

    LOG.info(String.format("UPDATE REGION TRANSITION serverName=%s region=%s state=%s",
        serverName, regionNode, state));

    final ServerStateNode serverNode = regionStateMap.getOrCreateServer(serverName);
    if (!reportTransition(regionNode, serverNode, state, seqId)) {
      LOG.warn(String.format(
        "no procedure found for region=%s. server=%s was trying to transition to %s",
        regionNode, serverName, state));
    }
  }

  private boolean reportTransition(final RegionStateNode regionNode,
      final ServerStateNode serverNode, final TransitionCode state, final long seqId)
      throws UnexpectedStateException {
    final ServerName serverName = serverNode.getServerName();
    synchronized (regionNode) {
      final RegionTransitionProcedure proc = regionNode.getProcedure();
      if (proc == null) return false;

      //serverNode.getReportEvent().removeProcedure(proc);
      proc.reportTransition(master.getMasterProcedureExecutor().getEnvironment(),
        serverName, state, seqId);
      return true;
    }
  }

  private void updateRegionSplitTransition(final ServerName serverName, final TransitionCode state,
      final HRegionInfo parent, final HRegionInfo hriA, final HRegionInfo hriB)
      throws PleaseHoldException, UnexpectedStateException, IOException {
    checkFailoverCleanupCompleted(parent);

    if (state != TransitionCode.READY_TO_SPLIT) {
      throw new UnexpectedStateException("unsupported split state=" + state +
        " for parent region " + parent +
        " maybe an old RS (< 2.0) had the operation in progress");
    }

    // sanity check on the request
    if (!Bytes.equals(hriA.getEndKey(), hriB.getStartKey())) {
      throw new UnsupportedOperationException(
        "unsupported split request with bad keys: parent=" + parent +
        " hriA=" + hriA + " hriB=" + hriB);
    }

    try {
      regionStateListener.onRegionSplit(parent);
    } catch (QuotaExceededException e) {
      // TODO: does this really belong here?
      master.getRegionNormalizer().planSkipped(parent, PlanType.SPLIT);
      throw e;
    }

    // Submit split
    final byte[] splitKey = hriB.getStartKey();
    LOG.debug("handling split request from RS, parent=" + parent +
      " splitKey=" + Bytes.toStringBinary(splitKey));
    master.getMasterProcedureExecutor().submitProcedure(
        createSplitProcedure(parent, splitKey));

    // TODO: If the RS is < 2.0 throw an exception, we are handling the split
  }

  private void updateRegionMergeTransition(final ServerName serverName, final TransitionCode state,
      final HRegionInfo merged, final HRegionInfo hriA, final HRegionInfo hriB)
      throws PleaseHoldException, UnexpectedStateException {
    checkFailoverCleanupCompleted(merged);

    // TODO: Attach merge support
    throw new UnsupportedOperationException(String.format(
      "Merge not handled yet: state=%s merged=%s hriA=%s hriB=%s", state, merged, hriA, hriB));
  }

  // ============================================================================================
  //  RS Status update (report online regions) helpers
  // ============================================================================================
  /**
   * the master will call this method when the RS send the regionServerReport().
   * the report will contains the "hbase version" and the "online regions".
   * this method will check the the online regions against the in-memory state of the AM,
   * if there is a mismatch we will try to fence out the RS with the assumption
   * that something went wrong on the RS side.
   */
  public void reportOnlineRegions(final ServerName serverName,
      final int versionNumber, final Set<byte[]> regionNames) {
    if (!isRunning()) return;

    final long startTime = EnvironmentEdgeManager.currentTime();
    final ServerStateNode serverNode = regionStateMap.getOrCreateServer(serverName);
    LOG.warn("Report ONLINE REGIONS server=" + serverName + " region=" + regionNames.size() +
      " isMetaLoaded=" + isMetaLoaded());

    // update the server version number. This will be used for live upgrades.
    synchronized (serverNode) {
      serverNode.setVersionNumber(versionNumber);
      if (serverNode.isInState(ServerState.SPLITTING, ServerState.OFFLINE)) {
        LOG.warn("Got a report from a server result in state " + serverNode.getState());
        return;
      }
    }

    if (regionNames.isEmpty()) {
      // nothing to do if we don't have regions
      LOG.trace("no online region found on " + serverName);
    } else if (!isMetaLoaded()) {
      // if we are still on startup, discard the report unless is from someone holding meta
      checkOnlineRegionsReportForMeta(serverNode, regionNames);
    } else {
      // The Heartbeat updates us of what regions are only. check and verify the state.
      checkOnlineRegionsReport(serverNode, regionNames);
    }

    // wake report event
    wakeServerReportEvent(serverNode);

    // update metrics
    metrics.updateOnlineReportTime(EnvironmentEdgeManager.currentTime() - startTime);
  }

  public void checkOnlineRegionsReportForMeta(final ServerStateNode serverNode,
      final Set<byte[]> regionNames) {
    try {
      for (byte[] regionName: regionNames) {
        final HRegionInfo hri = getMetaRegionFromName(regionName);
        if (hri == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skip online report for region=" + Bytes.toStringBinary(regionName) +
              " while meta is loading");
          }
          continue;
        }

        final RegionStateNode regionNode = regionStateMap.getOrCreateRegionNode(hri);
        LOG.info("META REPORTED: " + regionNode);
        if (!reportTransition(regionNode, serverNode, TransitionCode.OPENED, 0)) {
          LOG.warn("meta reported but no procedure found");
          regionNode.setRegionLocation(serverNode.getServerName());
        }
      }
    } catch (UnexpectedStateException e) {
      final ServerName serverName = serverNode.getServerName();
      LOG.warn("Killing server=" + serverName + ": " + e.getMessage());
      killRegionServer(serverNode);
    }
  }

  public void checkOnlineRegionsReport(final ServerStateNode serverNode,
      final Set<byte[]> regionNames) {
    final ServerName serverName = serverNode.getServerName();
    try {
      for (byte[] regionName: regionNames) {
        if (!isRunning()) return;

        final RegionStateNode regionNode = regionStateMap.getRegionNodeFromName(regionName);
        if (regionNode == null) {
          throw new UnexpectedStateException(
            "Reported online region " + Bytes.toStringBinary(regionName) + " not found");
        }

        synchronized (regionNode) {
          if (regionNode.isInState(State.OPENING, State.OPEN)) {
            if (!regionNode.getRegionLocation().equals(serverName)) {
              throw new UnexpectedStateException(
                "Reported OPEN region on server=" + serverName +
                " but the state found says server=" + regionNode.getRegionLocation());
            } else if (regionNode.isInState(State.OPENING)) {
              try {
                if (!reportTransition(regionNode, serverNode, TransitionCode.OPENED, 0)) {
                  LOG.warn("Reported OPEN region on server=" + serverName +
                    " but the state found says " + regionNode + " and no procedure is running");
                }
              } catch (UnexpectedStateException e) {
                LOG.warn("unexpected exception while trying to report " + regionNode +
                  " as open: " + e.getMessage(), e);
              }
            }
          } else if (!regionNode.isInState(State.CLOSING, State.SPLITTING)) {
            // TODO: We end up killing the RS if we get a report while we already
            // transitioned to close or split. we should have a timeout/timestamp to compare
            throw new UnexpectedStateException(
                "Reported OPEN region, but the state found says " + regionNode.getState());
          }
        }
      }
    } catch (UnexpectedStateException e) {
      LOG.warn("Killing server=" + serverName + ": " + e.getMessage());
      killRegionServer(serverNode);
    }
  }

  protected boolean waitServerReportEvent(final ServerName serverName, final Procedure proc) {
    final ServerStateNode serverNode = regionStateMap.getOrCreateServer(serverName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("wait for server " + serverName + " report " + proc);
    }
    return getProcedureScheduler().waitEvent(serverNode.getReportEvent(), proc);
  }

  protected void wakeServerReportEvent(final ServerStateNode serverNode) {
    getProcedureScheduler().wakeEvent(serverNode.getReportEvent());
  }

  // ============================================================================================
  //  RIT chore
  // ============================================================================================
  private static class RegionInTransitionChore extends ProcedureInMemoryChore<MasterProcedureEnv> {
    public RegionInTransitionChore(final int timeoutMsec) {
      super(timeoutMsec);
    }

    @Override
    protected void periodicExecute(final MasterProcedureEnv env) {
      final AssignmentManager am = env.getAssignmentManager();

      final RegionInTransitionStat ritStat = am.computeRegionInTransitionStat();
      if (ritStat.hasRegionsOverThreshold()) {
        for (RegionState hri: ritStat.getRegionOverThreshold()) {
          am.handleRegionOverStuckWarningThreshold(hri.getRegion());
        }
      }

      // update metrics
      am.updateRegionsInTransitionMetrics(ritStat);
    }
  }

  public RegionInTransitionStat computeRegionInTransitionStat() {
    final RegionInTransitionStat rit = new RegionInTransitionStat(getConfiguration());
    rit.update(this);
    return rit;
  }

  public static class RegionInTransitionStat {
    private final int ritThreshold;

    private HashMap<String, RegionState> ritsOverThreshold = null;
    private long statTimestamp;
    private long oldestRITTime = 0;
    private int totalRITsTwiceThreshold = 0;
    private int totalRITs = 0;

    protected RegionInTransitionStat(final Configuration conf) {
      this.ritThreshold =
        conf.getInt(METRICS_RIT_STUCK_WARNING_THRESHOLD, DEFAULT_RIT_STUCK_WARNING_THRESHOLD);
    }

    public int getRITThreshold() {
      return ritThreshold;
    }

    public long getTimestamp() {
      return statTimestamp;
    }

    public int getTotalRITs() {
      return totalRITs;
    }

    public long getOldestRITTime() {
      return oldestRITTime;
    }

    public int getTotalRITsOverThreshold() {
      return ritsOverThreshold != null ? ritsOverThreshold.size() : 0;
    }

    public boolean hasRegionsTwiceOverThreshold() {
      return totalRITsTwiceThreshold > 0;
    }

    public boolean hasRegionsOverThreshold() {
      return ritsOverThreshold != null && !ritsOverThreshold.isEmpty();
    }

    public Collection<RegionState> getRegionOverThreshold() {
      return ritsOverThreshold.values();
    }

    public boolean isRegionOverThreshold(final HRegionInfo regionInfo) {
      return ritsOverThreshold.containsKey(regionInfo.getEncodedName());
    }

    public boolean isRegionTwiceOverThreshold(final HRegionInfo regionInfo) {
      final RegionState state = ritsOverThreshold.get(regionInfo);
      if (state == null) return false;
      return (statTimestamp - state.getStamp()) > (ritThreshold * 2);
    }

    protected void update(final AssignmentManager am) {
      final RegionStates regionStates = am.getRegionStates();
      this.statTimestamp = EnvironmentEdgeManager.currentTime();
      update(regionStates.getRegionsStateInTransition(), statTimestamp);
      update(regionStates.getRegionFailedOpen(), statTimestamp);
    }

    private void update(final Collection<RegionState> regions, final long currentTime) {
      for (RegionState state: regions) {
        totalRITs++;
        final long ritTime = currentTime - state.getStamp();
        if (ritTime > ritThreshold) {
          if (ritsOverThreshold == null) {
            ritsOverThreshold = new HashMap<String, RegionState>();
          }
          ritsOverThreshold.put(state.getRegion().getEncodedName(), state);
          totalRITsTwiceThreshold += (ritTime > (ritThreshold * 2)) ? 1 : 0;
        }
        if (oldestRITTime < ritTime) {
          oldestRITTime = ritTime;
        }
      }
    }
  }

  private void updateRegionsInTransitionMetrics(final RegionInTransitionStat ritStat) {
    metrics.updateRITOldestAge(ritStat.getOldestRITTime());
    metrics.updateRITCount(ritStat.getTotalRITs());
    metrics.updateRITCountOverThreshold(ritStat.getTotalRITsOverThreshold());
  }

  private void handleRegionOverStuckWarningThreshold(final HRegionInfo regionInfo) {
    final RegionStateNode regionNode = regionStateMap.getRegionNode(regionInfo);
    LOG.warn("TODO Handle region stuck in transition: " + regionNode);
  }

  // ============================================================================================
  //  TODO: Master load/bootstrap
  // ============================================================================================
  public void joinCluster() throws IOException {
    final long startTime = System.currentTimeMillis();

    LOG.info("Joining the cluster...");

    /*
    int poolSize = master.getConfiguration().getInt(BOOTSTRAP_THREAD_POOL_SIZE_CONF_KEY,
        DEFAULT_BOOTSTRAP_THREAD_POOL_SIZE);
    TaskPool taskPool = TaskPool(poolSize, 60, TimeUnit.SECONDS, "AssignmentManager-JoinCluster");
    */

    // Scan hbase:meta to build list of existing regions, servers, and assignment
    loadMeta();

    for (int i = 0; master.getServerManager().countOfRegionServers() < 1; ++i) {
      LOG.info("waiting for RS to join");
      Threads.sleep(250);
    }
    LOG.info("RS joined " + master.getServerManager().countOfRegionServers());

    // This method will assign all user regions if a clean server startup or
    // it will reconstruct master state and cleanup any leftovers from previous master process.
    boolean failover = processofflineServersWithOnlineRegions();

    // Start the RIT chore
    master.getMasterProcedureExecutor().addChore(this.ritChore);

    LOG.info(String.format("Joined the cluster in %s, failover=%s",
      StringUtils.humanTimeDiff(System.currentTimeMillis() - startTime), failover));
  }

  private void loadMeta() throws IOException {
    // TODO: use a thread pool
    regionStateStore.visitMeta(new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(final HRegionInfo regionInfo, final State state,
          final ServerName regionLocation, final ServerName lastHost, final long openSeqNum) {
        final RegionStateNode regionNode = regionStateMap.getOrCreateRegionNode(regionInfo);
        synchronized (regionNode) {
          if (!regionNode.isInTransition()) {
            regionNode.setState(state);
            regionNode.setLastHost(lastHost);
            regionNode.setRegionLocation(regionLocation);
            regionNode.setOpenSeqNum(openSeqNum);

            if (state == State.OPEN) {
              assert regionLocation != null : "found null region location for " + regionNode;
              regionStateMap.addRegionToServer(regionLocation, regionNode);
            } else if (state == State.OFFLINE || regionInfo.isOffline()) {
              regionStateMap.addToOfflineRegions(regionNode);
            } else {
              // These regions should have a procedure in replay
              regionStateMap.addRegionInTransition(regionNode, null);
            }
          }
        }
      }
    });

    // every assignment is blocked until meta is loaded.
    wakeMetaLoadedEvent();
  }

  // TODO: the assumption here is that if RSs are crashing while we are executing this
  // they will be handled by the SSH that will be putted in the ServerManager "queue".
  // we can integrate this a bit better.
  private boolean processofflineServersWithOnlineRegions() {
    boolean failover = !master.getServerManager().getDeadServers().isEmpty();

    final Set<ServerName> offlineServersWithOnlineRegions = new HashSet<ServerName>();
    final ArrayList<HRegionInfo> regionsToAssign = new ArrayList<HRegionInfo>();
    long st, et;

    st = System.currentTimeMillis();
    for (RegionStateNode regionNode: regionStateMap.getRegionNodes()) {
      if (regionNode.getState() == State.OPEN) {
        final ServerName serverName = regionNode.getRegionLocation();
        if (!master.getServerManager().isServerOnline(serverName)) {
          offlineServersWithOnlineRegions.add(serverName);
        }
      } else if (regionNode.getState() == State.OFFLINE) {
        if (isTableEnabled(regionNode.getTable())) {
          regionsToAssign.add(regionNode.getRegionInfo());
        }
      }
    }
    et = System.currentTimeMillis();
    LOG.info("[STEP-1] " + StringUtils.humanTimeDiff(et - st));

    // kill servers with online regions
    st = System.currentTimeMillis();
    for (ServerName serverName: offlineServersWithOnlineRegions) {
      if (!master.getServerManager().isServerOnline(serverName)) {
        LOG.info("KILL RS hosting regions but not online " + serverName +
          " (master=" + master.getServerName() + ")");
        killRegionServer(serverName);
      }
    }
    et = System.currentTimeMillis();
    LOG.info("[STEP-2] " + StringUtils.humanTimeDiff(et - st));

    setFailoverCleanupDone(true);

    // assign offline regions
    st = System.currentTimeMillis();
    for (HRegionInfo regionInfo: regionsToAssign) {
      master.getMasterProcedureExecutor().submitProcedure(
        createAssignProcedure(regionInfo, false));
    }
    et = System.currentTimeMillis();
    LOG.info("[STEP-3] " + StringUtils.humanTimeDiff(et - st));

    return failover;
  }

  /**
   * Used by ServerCrashProcedure to make sure AssignmentManager has completed
   * the failover cleanup before re-assigning regions of dead servers. So that
   * when re-assignment happens, AssignmentManager has proper region states.
   */
  public boolean isFailoverCleanupDone() {
    return failoverCleanupDone.isReady();
  }

  /**
   * Used by ServerCrashProcedure tests verify the ability to suspend the
   * execution of the ServerCrashProcedure.
   */
  @VisibleForTesting
  public void setFailoverCleanupDone(final boolean b) {
    master.getMasterProcedureExecutor().getEnvironment()
      .setEventReady(failoverCleanupDone, b);
  }

  public ProcedureEvent getFailoverCleanupEvent() {
    return failoverCleanupDone;
  }

  /**
   * Used to check if the failover cleanup is done.
   * if not we throw PleaseHoldException since we are rebuilding the RegionStates
   * @param hri region to check if it is already rebuild
   * @throws PleaseHoldException if the failover cleanup is not completed
   */
  private void checkFailoverCleanupCompleted(final HRegionInfo hri) throws PleaseHoldException {
    if (!isRunning()) {
      throw new PleaseHoldException("AssignmentManager is not running");
    }

    // TODO: can we avoid throwing an exception if hri is already loaded?
    //       at the moment we bypass only meta
    if (!isMetaRegion(hri) && !isFailoverCleanupDone()) {
      LOG.warn("Master is rebuilding user regions: " + hri);
      throw new PleaseHoldException("Master is rebuilding user regions");
    }
  }

  // ============================================================================================
  //  TODO: Metrics
  // ============================================================================================
  public int getNumRegionsOpened() {
    // TODO: Used by TestRegionPlacement.java and assume monotonically increasing value
    return 0;
  }

  // TODO: can this stuff be only in the AssignProcedure and available by getting only RIT?
  private Map<HRegionInfo, AtomicInteger> failedTracker =
    new java.util.concurrent.ConcurrentHashMap<HRegionInfo, AtomicInteger>();
  public Map<HRegionInfo, AtomicInteger> getFailedOpenTracker() {
    return failedTracker;
  }

  protected int incrementAndGetFailedOpen(final HRegionInfo regionInfo) {
    Map<HRegionInfo, AtomicInteger> failedOpen = getFailedOpenTracker();
    AtomicInteger count = failedOpen.get(regionInfo);
    if (count == null) {
      count = new AtomicInteger(1);
      failedOpen.put(regionInfo, count);
      return 1;
    }
    return count.incrementAndGet();
  }

  // ============================================================================================
  //  TODO: Server Crash
  // ============================================================================================
  public void offlineRegion(final HRegionInfo regionInfo) throws IOException {
    // TODO used by MasterRpcServices ServerCrashProcedure
    LOG.info("OFFLINE REGION " + regionInfo);
    final RegionStateNode node = regionStateMap.getRegionNode(regionInfo);
    if (node != null) {
      node.setState(State.OFFLINE);
      node.setRegionLocation(null);
    }
  }

  public Map<ServerName, List<HRegionInfo>> getSnapShotOfAssignment(
      final Collection<HRegionInfo> regions) {
    return regionStateMap.getSnapShotOfAssignment(regions);
  }

  // ============================================================================================
  //  TODO: UTILS/HELPERS?
  // ============================================================================================
  /**
   * Used by the client (via master) to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command (pending/total)
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(TableName tableName)
      throws IOException {
    if (isTableDisabled(tableName)) return new Pair<Integer, Integer>(0, 0);

    final List<RegionState> states = regionStateMap.getTableRegionStates(tableName);
    int ritCount = 0;
    for (RegionState regionState: states) {
      if (!regionState.isOpened()) ritCount++;
    }
    return new Pair<Integer, Integer>(ritCount, states.size());
  }

  // ============================================================================================
  //  TODO: Region State In Transition
  // ============================================================================================
  protected boolean addRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    return regionStateMap.addRegionInTransition(regionNode, procedure);
  }

  protected void removeRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    regionStateMap.removeRegionInTransition(regionNode, procedure);
  }

  public boolean hasRegionsInTransition() {
    return regionStateMap.hasRegionsInTransition();
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return regionStateMap.getRegionsInTransition();
  }

  public List<HRegionInfo> getAssignedRegions() {
    return regionStateMap.getAssignedRegions();
  }

  public HRegionInfo getRegionInfo(final byte[] regionName) {
    final RegionStateNode regionState = regionStateMap.getRegionNodeFromName(regionName);
    return regionState != null ? regionState.getRegionInfo() : null;
  }

  // ============================================================================================
  //  TODO: Region Status update
  // ============================================================================================
  public void markRegionAsOpening(final RegionStateNode regionNode) throws IOException {
    LOG.info("TODO: MARK REGION AS OPENING " + regionNode);
    synchronized (regionNode) {
      if (!regionNode.setState(State.OPENING, RegionStates.STATES_EXPECTEX_IN_OPEN)) {
        throw new UnexpectedStateException(
          "unexpected state " + regionNode.getState() + " for region " + regionNode);
      }

      // TODO: Do we need to update the state
      regionStateStore.updateRegionLocation(regionNode.getRegionInfo(), State.OPENING,
        regionNode.getRegionLocation(), regionNode.getLastHost(), HConstants.NO_SEQNUM);
    }
  }

  public void markRegionAsOpened(final RegionStateNode regionNode) throws IOException {
    final HRegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      if (!regionNode.setState(State.OPEN, RegionStates.STATES_EXPECTEX_IN_OPEN)) {
        throw new UnexpectedStateException(
          "unexpected state " + regionNode.getState() + " for region " + regionNode);
      }

      // TODO: Update Meta
      if (isMetaRegion(hri)) {
        setMetaInitialized(hri, true);
      }

      // TODO
      LOG.info("TODO: MARK REGION AS OPEN " + regionNode);
      regionStateMap.addRegionToServer(regionNode.getRegionLocation(), regionNode);
      regionStateStore.updateRegionLocation(regionNode.getRegionInfo(), State.OPEN,
        regionNode.getRegionLocation(), regionNode.getLastHost(), regionNode.getOpenSeqNum());

      sendRegionOpenedNotification(hri, regionNode.getRegionLocation());

      // update assignment metrics
      if (regionNode.getProcedure() != null) {
        metrics.updateAssignTime(regionNode.getProcedure().elapsedTime());
      }
    }
  }

  public void markRegionAsClosing(final RegionStateNode regionNode) throws IOException {
    LOG.info("TODO: MARK REGION AS CLOSING " + regionNode);
    final HRegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      if (!regionNode.setState(State.CLOSING, RegionStates.STATES_EXPECTEX_IN_CLOSE)) {
        throw new UnexpectedStateException(
          "unexpected state " + regionNode.getState() + " for region " + regionNode);
      }

      // set meta has not initialized early. so people trying to create/edit tables will wait
      if (isMetaRegion(hri)) {
        setMetaInitialized(hri, false);
      }

      regionStateStore.updateRegionLocation(regionNode.getRegionInfo(), State.CLOSING,
        regionNode.getRegionLocation(), regionNode.getLastHost(), HConstants.NO_SEQNUM);
    }
  }

  public void markRegionAsClosed(final RegionStateNode regionNode) throws IOException {
    LOG.info("TODO: MARK REGION AS CLOSED " + regionNode);
    final HRegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      if (!regionNode.setState(State.CLOSED, RegionStates.STATES_EXPECTEX_IN_CLOSE)) {
        throw new UnexpectedStateException(
          "unexpected state " + regionNode.getState() + " for region " + regionNode);
      }

      regionStateMap.removeRegionFromServer(regionNode.getRegionLocation(), regionNode);
      regionStateStore.updateRegionLocation(regionNode.getRegionInfo(), State.CLOSED,
        regionNode.getRegionLocation(), regionNode.getLastHost(), HConstants.NO_SEQNUM);

      sendRegionClosedNotification(hri);

      // update assignment metrics
      if (regionNode.getProcedure() != null) {
        metrics.updateUnassignTime(regionNode.getProcedure().elapsedTime());
      }
    }
  }

  private void sendRegionOpenedNotification(final HRegionInfo regionInfo,
      final ServerName serverName) {
    getBalancer().regionOnline(regionInfo, serverName);
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionOpened(regionInfo, serverName);
      }
    }
  }

  private void sendRegionClosedNotification(final HRegionInfo regionInfo) {
    getBalancer().regionOffline(regionInfo);
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionClosed(regionInfo);
      }
    }
  }

  public void markRegionAsSplitted(final HRegionInfo parent, final ServerName serverName,
      final HRegionInfo hriA, final HRegionInfo hriB) throws IOException {
    parent.setSplit(true);
    final RegionStateNode node = regionStateMap.getOrCreateRegionNode(parent);
    node.getRegionInfo().setSplit(true);
    node.getRegionInfo().setOffline(true);
    node.setState(State.SPLIT);

    LOG.info("TODO: MARK REGION AS SPLIT parent=" + parent);
    regionStateStore.splitRegion(parent, hriA, hriB, serverName);
  }

  // ============================================================================================
  //  Assign Queue (Assign/Balance)
  // ============================================================================================
  private final ArrayList<RegionStateNode> pendingAssignQueue = new ArrayList<RegionStateNode>();
  private final ReentrantLock assignQueueLock = new ReentrantLock();
  private final Condition assignQueueFullCond = assignQueueLock.newCondition();

  /**
   * Add the assign operation to the assignment queue.
   * The pending assignment operation will be processed,
   * and each region will be assigned by a server using the balancer.
   */
  protected void queueAssign(final RegionStateNode regionNode) {
    getProcedureScheduler().suspendEvent(regionNode.getProcedureEvent());

    // TODO: quick-start for meta and the other sys-tables?
    assignQueueLock.lock();
    try {
      pendingAssignQueue.add(regionNode);
      if (regionNode.isSystemTable() ||
          pendingAssignQueue.size() == 1 ||
          pendingAssignQueue.size() >= assignDispatchWaitQueueMaxSize) {
        assignQueueFullCond.signal();
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  private void startAssignmentThread() {
    assignThread = new Thread("AssignmentThread") {
      @Override
      public void run() {
        while (isRunning()) {
          processAssignQueue();
        }
        pendingAssignQueue.clear();
      }
    };
    assignThread.start();
  }

  private void stopAssignmentThread() {
    assignQueueSignal();
    try {
      while (assignThread.isAlive()) {
        assignQueueSignal();
        assignThread.join(250);
      }
    } catch (InterruptedException e) {
      LOG.warn("join interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private void assignQueueSignal() {
    assignQueueLock.lock();
    try {
      assignQueueFullCond.signal();
    } finally {
      assignQueueLock.unlock();
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  private HashMap<HRegionInfo, RegionStateNode> waitOnAssignQueue() {
    HashMap<HRegionInfo, RegionStateNode> regions = null;

    assignQueueLock.lock();
    try {
      if (pendingAssignQueue.isEmpty() && isRunning()) {
        assignQueueFullCond.await();
      }

      if (!isRunning()) return null;
      assignQueueFullCond.await(assignDispatchWaitMillis, TimeUnit.MILLISECONDS);
      regions = new HashMap<HRegionInfo, RegionStateNode>(pendingAssignQueue.size());
      for (RegionStateNode regionNode: pendingAssignQueue) {
        regions.put(regionNode.getRegionInfo(), regionNode);
      }
      pendingAssignQueue.clear();
    } catch (InterruptedException e) {
      LOG.warn("got interrupted ", e);
      Thread.currentThread().interrupt();
    } finally {
      assignQueueLock.unlock();
    }
    return regions;
  }

  private void processAssignQueue() {
    final HashMap<HRegionInfo, RegionStateNode> regions = waitOnAssignQueue();
    if (regions == null || regions.size() == 0 || !isRunning()) {
      return;
    }

    LOG.info("PROCESS ASSIGN QUEUE " + regions.size());

    // TODO: Optimize balancer. pass a RegionPlan?
    final HashMap<HRegionInfo, ServerName> retainMap = new HashMap<HRegionInfo, ServerName>();
    final List<HRegionInfo> rrList = new ArrayList<HRegionInfo>();
    for (RegionStateNode regionNode: regions.values()) {
      if (regionNode.getRegionLocation() != null) {
        retainMap.put(regionNode.getRegionInfo(), regionNode.getRegionLocation());
      } else {
        rrList.add(regionNode.getRegionInfo());
      }
    }

    // TODO: connect with the listener to invalidate the cache
    final LoadBalancer balancer = getBalancer();

    // TODO use events
    List<ServerName> servers = master.getServerManager().createDestinationServersList();
    for (int i = 0; servers.size() < 1; ++i) {
      if (i % 4 == 0) {
        LOG.warn("no server available, unable to find a location for " + regions.size() +
            " unassigned regions. waiting");
      }

      // the was AM killed
      if (!isRunning()) {
        LOG.debug("aborting assignment-queue with " + regions.size() + " not assigned");
        return;
      }

      Threads.sleep(250);
      servers = master.getServerManager().createDestinationServersList();
    }

    final boolean isTraceEnabled = LOG.isTraceEnabled();
    if (isTraceEnabled) {
      LOG.trace("available servers count=" + servers.size() + ": " + servers);
    }

    // ask the balancer where to place regions
    if (!retainMap.isEmpty()) {
      if (isTraceEnabled) {
        LOG.trace("retain assign regions=" + retainMap);
      }
      try {
        acceptPlan(regions, balancer.retainAssignment(retainMap, servers));
      } catch (HBaseIOException e) {
        LOG.warn("unable to retain assignment", e);
        addToPendingAssignment(regions, retainMap.keySet());
      }
    }

    // TODO: Do we need to split retain and round-robin?
    // the retain seems to fallback to round-robin/random if the region is not in the map.
    if (!rrList.isEmpty()) {
      Collections.sort(rrList);
      if (isTraceEnabled) {
        LOG.trace("round robin regions=" + rrList);
      }
      try {
        acceptPlan(regions, balancer.roundRobinAssignment(rrList, servers));
      } catch (HBaseIOException e) {
        LOG.warn("unable to round-robin assignment", e);
        addToPendingAssignment(regions, rrList);
      }
    }
  }

  private void acceptPlan(final HashMap<HRegionInfo, RegionStateNode> regions,
      final Map<ServerName, List<HRegionInfo>> plan) throws HBaseIOException {
    final ProcedureEvent[] events = new ProcedureEvent[regions.size()];
    final long st = System.currentTimeMillis();

    if (plan == null) {
      throw new HBaseIOException("unable to compute plans for regions=" + regions.size());
    }

    if (plan.isEmpty()) return;

    int evcount = 0;
    for (Map.Entry<ServerName, List<HRegionInfo>> entry: plan.entrySet()) {
      final ServerName server = entry.getKey();
      for (HRegionInfo hri: entry.getValue()) {
        final RegionStateNode regionNode = regions.get(hri);
        regionNode.setRegionLocation(server);
        events[evcount++] = regionNode.getProcedureEvent();
      }
    }
    getProcedureScheduler().wakeEvents(evcount, events);

    final long et = System.currentTimeMillis();
    LOG.info("ASSIGN ACCEPT " + events.length + " -> " + StringUtils.humanTimeDiff(et - st));
  }

  private void addToPendingAssignment(final HashMap<HRegionInfo, RegionStateNode> regions,
      final Collection<HRegionInfo> pendingRegions) {
    assignQueueLock.lock();
    try {
      for (HRegionInfo hri: pendingRegions) {
        pendingAssignQueue.add(regions.get(hri));
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  // ============================================================================================
  //  Server Helpers
  // ============================================================================================
  @Override
  public void serverAdded(final ServerName serverName) {
  }

  @Override
  public void serverRemoved(final ServerName serverName) {
    final ServerStateNode serverNode = regionStateMap.getServerNode(serverName);
    if (serverNode == null) return;

    // just in case, wake procedures waiting for this server report
    wakeServerReportEvent(serverNode);
  }

  public int getServerVersion(final ServerName serverName) {
    final ServerStateNode node = regionStateMap.getServerNode(serverName);
    return node != null ? node.getVersionNumber() : 0;
  }

  public void killRegionServer(final ServerName serverName) {
    final ServerStateNode serverNode = regionStateMap.getServerNode(serverName);
    killRegionServer(serverNode);
  }

  public void killRegionServer(final ServerStateNode serverNode) {
    for (RegionStateNode regionNode: serverNode.getRegions()) {
      regionNode.setState(State.OFFLINE);
      regionNode.setRegionLocation(null);
    }
    master.getServerManager().expireServer(serverNode.getServerName());
  }
}
