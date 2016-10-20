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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * RegionStates contains a set of maps that describes the in-memory state of the AM, with
 * the regions available in the system, the region in transition, the offline regions and
 * the servers holding regions.
 */
@InterfaceAudience.Private
public class RegionStates {
  private static final Log LOG = LogFactory.getLog(RegionStates.class);

  protected static final State[] STATES_EXPECTEX_IN_OPEN = new State[] {
    State.OFFLINE, State.CLOSED,      // disable/offline
    State.SPLITTING, State.SPLIT,     // ServerCrashProcedure
    State.OPENING, State.FAILED_OPEN, // already in-progress (retrying)
  };

  protected static final State[] STATES_EXPECTEX_IN_CLOSE = new State[] {
    State.SPLITTING, State.SPLIT, // ServerCrashProcedure
    State.OPEN,                   // enabled/open
    State.CLOSING                 // already in-progress (retrying)
  };

  private static class AssignmentProcedureEvent extends ProcedureEvent<HRegionInfo> {
    public AssignmentProcedureEvent(final HRegionInfo regionInfo) {
      super(regionInfo);
    }
  }

  private static class ServerReportEvent extends ProcedureEvent<ServerName> {
    public ServerReportEvent(final ServerName serverName) {
      super(serverName);
    }
  }

  public static class RegionStateNode implements Comparable<RegionStateNode> {
    private final HRegionInfo regionInfo;
    private final ProcedureEvent event;

    private volatile RegionTransitionProcedure procedure = null;
    private volatile ServerName regionLocation = null;
    private volatile ServerName lastHost = null;
    private volatile State state = State.OFFLINE;
    private volatile long lastUpdate = 0;
    private volatile long openSeqNum = HConstants.NO_SEQNUM;

    public RegionStateNode(final HRegionInfo regionInfo) {
      this.regionInfo = regionInfo;
      this.event = new AssignmentProcedureEvent(regionInfo);
    }

    public boolean setState(final State update, final State... expected) {
      final boolean inExpectedState = isInState(expected);
      if (inExpectedState) {
        this.state = update;
      }
      return inExpectedState;
    }

    public boolean isInState(final State... expected) {
      if (expected != null && expected.length > 0) {
        boolean inExpectedState = false;
        for (int i = 0; i < expected.length; ++i) {
          inExpectedState |= (state == expected[i]);
        }
        return inExpectedState;
      }
      return true;
    }

    public boolean isInTransition() {
      return getProcedure() != null;
    }

    public long getLastUpdate() {
      return procedure != null ? procedure.getLastUpdate() : lastUpdate;
    }

    public void setLastHost(final ServerName serverName) {
      this.lastHost = serverName;
    }

    public void setOpenSeqNum(final long seqId) {
      this.openSeqNum = seqId;
    }

    public void setRegionLocation(final ServerName serverName) {
      this.regionLocation = serverName;
      this.lastUpdate = EnvironmentEdgeManager.currentTime();
    }

    public boolean setProcedure(final RegionTransitionProcedure proc) {
      if (this.procedure != null && this.procedure != proc) {
        return false;
      }
      this.procedure = proc;
      return true;
    }

    public boolean unsetProcedure(final RegionTransitionProcedure proc) {
      if (this.procedure != null && this.procedure != proc) {
        return false;
      }
      this.procedure = null;
      return true;
    }

    public RegionTransitionProcedure getProcedure() {
      return procedure;
    }

    public ProcedureEvent getProcedureEvent() {
      return event;
    }

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    public TableName getTable() {
      return getRegionInfo().getTable();
    }

    public boolean isSystemTable() {
      return getTable().isSystemTable();
    }

    public ServerName getLastHost() {
      return lastHost;
    }

    public ServerName getRegionLocation() {
      return regionLocation;
    }

    public State getState() {
      return state;
    }

    public long getOpenSeqNum() {
      return openSeqNum;
    }

    public int getFormatVersion() {
      // we don't have any format for now
      // it should probably be in regionInfo.getFormatVersion()
      return 0;
    }

    @Override
    public int compareTo(final RegionStateNode other) {
      // NOTE: HRegionInfo sort by table first, so we are relying on that.
      // we have a TestRegionState#testOrderedByTable() that check for that.
      return getRegionInfo().compareTo(other.getRegionInfo());
    }

    @Override
    public int hashCode() {
      return getRegionInfo().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) return true;
      if (!(other instanceof RegionStateNode)) return false;
      return compareTo((RegionStateNode)other) == 0;
    }

    @Override
    public String toString() {
      return toDescriptiveString();
    }

    public String toDescriptiveString() {
      return String.format("RegionStateNode(state=%s table=%s region=%s server=%s)",
        getState(), getTable(), getRegionInfo().getEncodedName(), getRegionLocation());
    }
  }

  // This comparator sorts the RegionStates by time stamp then Region name.
  // Comparing by timestamp alone can lead us to discard different RegionStates that happen
  // to share a timestamp.
  private static class RegionStateStampComparator implements Comparator<RegionState> {
    @Override
    public int compare(final RegionState l, final RegionState r) {
      int stampCmp = Long.compare(l.getStamp(), r.getStamp());
      return stampCmp != 0 ? stampCmp : l.getRegion().compareTo(r.getRegion());
    }
  }

  public enum ServerState { ONLINE, SPLITTING, OFFLINE }
  public static class ServerStateNode implements Comparable<ServerStateNode> {
    private final ServerReportEvent reportEvent;

    private final Set<RegionStateNode> regions;
    private final ServerName serverName;

    private volatile ServerState state = ServerState.ONLINE;
    private volatile int versionNumber = 0;

    public ServerStateNode(final ServerName serverName) {
      this.serverName = serverName;
      this.regions = new HashSet<RegionStateNode>();
      this.reportEvent = new ServerReportEvent(serverName);
    }

    public ServerName getServerName() {
      return serverName;
    }

    public ServerState getState() {
      return state;
    }

    public int getVersionNumber() {
      return versionNumber;
    }

    public ProcedureEvent getReportEvent() {
      return reportEvent;
    }

    public boolean isInState(final ServerState... expected) {
      boolean inExpectedState = false;
      if (expected != null) {
        for (int i = 0; i < expected.length; ++i) {
          inExpectedState |= (state == expected[i]);
        }
      }
      return inExpectedState;
    }

    public void setState(final ServerState state) {
      this.state = state;
    }

    public void setVersionNumber(final int versionNumber) {
      this.versionNumber = versionNumber;
    }

    public Set<RegionStateNode> getRegions() {
      return regions;
    }

    public int getRegionCount() {
      return regions.size();
    }

    public ArrayList<HRegionInfo> getRegionInfoList() {
      ArrayList<HRegionInfo> hris = new ArrayList<HRegionInfo>(regions.size());
      for (RegionStateNode region: regions) {
        hris.add(region.getRegionInfo());
      }
      return hris;
    }

    public void addRegion(final RegionStateNode regionNode) {
      this.regions.add(regionNode);
    }

    public void removeRegion(final RegionStateNode regionNode) {
      this.regions.remove(regionNode);
    }

    @Override
    public int compareTo(final ServerStateNode other) {
      return getServerName().compareTo(other.getServerName());
    }

    @Override
    public int hashCode() {
      return getServerName().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) return true;
      if (!(other instanceof ServerStateNode)) return false;
      return compareTo((ServerStateNode)other) == 0;
    }

    @Override
    public String toString() {
      return String.format("ServerStateNode(%s)", getServerName());
    }
  }

  public final static RegionStateStampComparator REGION_STATE_STAMP_COMPARATOR =
      new RegionStateStampComparator();

  // TODO: Replace the ConcurrentSkipListMaps
  private final ConcurrentSkipListMap<byte[], RegionStateNode> regionsMap =
      new ConcurrentSkipListMap<byte[], RegionStateNode>(Bytes.BYTES_COMPARATOR);

  private final ConcurrentSkipListMap<HRegionInfo, RegionStateNode> regionInTransition =
    new ConcurrentSkipListMap<HRegionInfo, RegionStateNode>();

  private final ConcurrentSkipListMap<HRegionInfo, RegionStateNode> regionOffline =
    new ConcurrentSkipListMap<HRegionInfo, RegionStateNode>();

  private final ConcurrentHashMap<ServerName, ServerStateNode> serverMap =
      new ConcurrentHashMap<ServerName, ServerStateNode>();

  public RegionStates() { }

  public void clear() {
    regionsMap.clear();
    regionInTransition.clear();
    regionOffline.clear();
    serverMap.clear();
  }

  // ==========================================================================
  //  RegionStateNode helpers
  // ==========================================================================
  protected RegionStateNode createRegionNode(final HRegionInfo regionInfo) {
    RegionStateNode newNode = new RegionStateNode(regionInfo);
    RegionStateNode oldNode = regionsMap.putIfAbsent(regionInfo.getRegionName(), newNode);
    return oldNode != null ? oldNode : newNode;
  }

  protected RegionStateNode getOrCreateRegionNode(final HRegionInfo regionInfo) {
    RegionStateNode node = regionsMap.get(regionInfo.getRegionName());
    return node != null ? node : createRegionNode(regionInfo);
  }

  public RegionStateNode getRegionNodeFromName(final byte[] regionName) {
    return regionsMap.get(regionName);
  }

  protected RegionStateNode getRegionNode(final HRegionInfo regionInfo) {
    return getRegionNodeFromName(regionInfo.getRegionName());
  }

  public RegionStateNode getRegionNodeFromEncodedName(final String encodedRegionName) {
    // TODO: Need a map <encodedName, ...> but it is just dispatch merge...
    for (RegionStateNode node: regionsMap.values()) {
      if (node.getRegionInfo().getEncodedName().equals(encodedRegionName)) {
        return node;
      }
    }
    return null;
  }

  public void deleteRegion(final HRegionInfo regionInfo) {
    regionsMap.remove(regionInfo.getRegionName());
  }

  public ArrayList<RegionStateNode> getTableRegionStateNodes(final TableName tableName) {
    final ArrayList<RegionStateNode> regions = new ArrayList<RegionStateNode>();
    for (RegionStateNode node: regionsMap.tailMap(tableName.getName()).values()) {
      if (!node.getTable().equals(tableName)) break;
      regions.add(node);
    }
    return regions;
  }

  public ArrayList<RegionState> getTableRegionStates(final TableName tableName) {
    final ArrayList<RegionState> regions = new ArrayList<RegionState>();
    for (RegionStateNode node: regionsMap.tailMap(tableName.getName()).values()) {
      if (!node.getTable().equals(tableName)) break;
      regions.add(createRegionState(node));
    }
    return regions;
  }

  public Collection<RegionStateNode> getRegionNodes() {
    return regionsMap.values();
  }

  public ArrayList<RegionState> getRegionStates() {
    final ArrayList<RegionState> regions = new ArrayList<RegionState>(regionsMap.size());
    for (RegionStateNode node: regionsMap.values()) {
      regions.add(createRegionState(node));
    }
    return regions;
  }

  // ==========================================================================
  //  RegionState helpers
  // ==========================================================================
  public RegionState getRegionState(final HRegionInfo regionInfo) {
    return createRegionState(getRegionNode(regionInfo));
  }

  public RegionState getRegionState(final String encodedRegionName) {
    return createRegionState(getRegionNodeFromEncodedName(encodedRegionName));
  }

  private RegionState createRegionState(final RegionStateNode node) {
    return node == null ? null :
      new RegionState(node.getRegionInfo(), node.getState(),
        node.getLastUpdate(), node.getRegionLocation());
  }

  // ============================================================================================
  //  TODO: helpers
  // ============================================================================================
  public boolean hasTableRegionStates(final TableName tableName) {
    // TODO
    return getTableRegionStates(tableName).size() > 0;
  }

  public List<HRegionInfo> getRegionsOfTable(final TableName table) {
    final ArrayList<RegionStateNode> nodes = getTableRegionStateNodes(table);
    final ArrayList<HRegionInfo> hris = new ArrayList<HRegionInfo>(nodes.size());
    for (RegionStateNode node: nodes) {
      hris.add(node.getRegionInfo());
    }
    return hris;
  }

  /**
   * Returns the set of regions hosted by the specified server
   * @param serverName the server we are interested in
   * @return set of HRegionInfo hosted by the specified server
   */
  public List<HRegionInfo> getServerRegionInfoSet(final ServerName serverName) {
    final ServerStateNode serverInfo = getServerNode(serverName);
    if (serverInfo == null) return Collections.emptyList();

    synchronized (serverInfo) {
      return serverInfo.getRegionInfoList();
    }
  }

  // ============================================================================================
  //  TODO: split helpers
  // ============================================================================================
  public void logSplit(final ServerName serverName) {
    final ServerStateNode serverNode = getOrCreateServer(serverName);
    synchronized (serverNode) {
      serverNode.setState(ServerState.SPLITTING);
      for (RegionStateNode regionNode: serverNode.getRegions()) {
        synchronized (regionNode) {
          // TODO: Abort procedure if present
          regionNode.setState(State.SPLITTING);
        }
      }
    }
  }

  public void logSplit(final HRegionInfo regionInfo) {
    final RegionStateNode regionNode = getRegionNode(regionInfo);
    synchronized (regionNode) {
      regionNode.setState(State.SPLIT);
    }
  }

  public void updateRegionState(final HRegionInfo regionInfo, final State state) {
    // TODO: Remove me, used by TestSplitTransactioOnCluster
  }

  // ============================================================================================
  //  TODO:
  // ============================================================================================
  public List<HRegionInfo> getAssignedRegions() {
    final List<HRegionInfo> result = new ArrayList<HRegionInfo>();
    for (RegionStateNode node: regionsMap.values()) {
      if (!node.isInTransition()) {
        result.add(node.getRegionInfo());
      }
    }
    return result;
  }

  public boolean isRegionInState(final HRegionInfo regionInfo, final State... state) {
    RegionStateNode region = getRegionNode(regionInfo);
    synchronized (region) {
      return region.isInState(state);
    }
  }

  public boolean isRegionOnline(final HRegionInfo regionInfo) {
    return isRegionInState(regionInfo, State.OPEN);
  }

  public Map<ServerName, List<HRegionInfo>> getSnapShotOfAssignment(
      final Collection<HRegionInfo> regions) {
    final Map<ServerName, List<HRegionInfo>> result = new HashMap<ServerName, List<HRegionInfo>>();
    for (HRegionInfo hri: regions) {
      final RegionStateNode node = getRegionNode(hri);
      if (node == null) continue;

      // TODO: State.OPEN
      final ServerName serverName = node.getRegionLocation();
      if (serverName == null) continue;

      List<HRegionInfo> serverRegions = result.get(serverName);
      if (serverRegions == null) {
        serverRegions = new ArrayList<HRegionInfo>();
        result.put(serverName, serverRegions);
      }

      serverRegions.add(node.getRegionInfo());
    }
    return result;
  }

  public Map<HRegionInfo, ServerName> getRegionAssignments() {
    final HashMap<HRegionInfo, ServerName> assignments = new HashMap<HRegionInfo, ServerName>();
    for (RegionStateNode node: regionsMap.values()) {
      assignments.put(node.getRegionInfo(), node.getRegionLocation());
    }
    return assignments;
  }

  public Map<RegionState.State, List<HRegionInfo>> getRegionByStateOfTable(TableName tableName) {
    final State[] states = State.values();
    final Map<RegionState.State, List<HRegionInfo>> tableRegions =
        new HashMap<State, List<HRegionInfo>>(states.length);
    for (int i = 0; i < states.length; ++i) {
      tableRegions.put(states[i], new ArrayList<HRegionInfo>());
    }

    for (RegionStateNode node: regionsMap.values()) {
      tableRegions.get(node.getState()).add(node.getRegionInfo());
    }
    return tableRegions;
  }

  public ServerName getRegionServerOfRegion(final HRegionInfo regionInfo) {
    RegionStateNode region = getRegionNode(regionInfo);
    synchronized (region) {
      return region.getRegionLocation();
    }
  }

  public Map<TableName, Map<ServerName, List<HRegionInfo>>> getAssignmentsByTable() {
    final Map<TableName, Map<ServerName, List<HRegionInfo>>> result =
      new HashMap<TableName, Map<ServerName, List<HRegionInfo>>>();
    for (RegionStateNode node: regionsMap.values()) {
      Map<ServerName, List<HRegionInfo>> tableResult = result.get(node.getTable());
      if (tableResult == null) {
        tableResult = new HashMap<ServerName, List<HRegionInfo>>();
        result.put(node.getTable(), tableResult);
      }

      final ServerName serverName = node.getRegionLocation();
      List<HRegionInfo> serverResult = tableResult.get(serverName);
      if (serverResult == null) {
        serverResult = new ArrayList<HRegionInfo>();
        tableResult.put(serverName, serverResult);
      }

      serverResult.add(node.getRegionInfo());
    }
    return result;
  }

  // ==========================================================================
  //  Region in transition helpers
  // ==========================================================================
  protected boolean addRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    if (procedure != null && !regionNode.setProcedure(procedure)) return false;

    regionInTransition.put(regionNode.getRegionInfo(), regionNode);
    return true;
  }

  protected void removeRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    regionInTransition.remove(regionNode.getRegionInfo());
    regionNode.unsetProcedure(procedure);
  }

  public boolean hasRegionsInTransition() {
    return !regionInTransition.isEmpty();
  }

  public boolean isRegionInTransition(final HRegionInfo regionInfo) {
    final RegionStateNode node = regionInTransition.get(regionInfo);
    return node != null ? node.isInTransition() : false;
  }

  public RegionState getRegionTransitionState(final HRegionInfo hri) {
    RegionStateNode node = regionInTransition.get(hri);
    if (node == null) return null;

    synchronized (node) {
      return node.isInTransition() ? createRegionState(node) : null;
    }
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return new ArrayList<RegionStateNode>(regionInTransition.values());
  }

  public List<RegionState> getRegionsStateInTransition() {
    final List<RegionState> rit = new ArrayList<RegionState>(regionInTransition.size());
    for (RegionStateNode node: regionInTransition.values()) {
      rit.add(createRegionState(node));
    }
    return rit;
  }

  public SortedSet<RegionState> getRegionsInTransitionOrderedByTimestamp() {
    final SortedSet<RegionState> rit = new TreeSet<RegionState>(REGION_STATE_STAMP_COMPARATOR);
    for (RegionStateNode node: regionInTransition.values()) {
      rit.add(createRegionState(node));
    }
    return rit;
  }

  // ==========================================================================
  //  Region offline helpers
  // ==========================================================================
  public void addToOfflineRegions(final RegionStateNode regionNode) {
    regionOffline.put(regionNode.getRegionInfo(), regionNode);
  }

  public void removeFromOfflineRegions(final HRegionInfo regionInfo) {
    regionOffline.remove(regionInfo);
  }

  // ==========================================================================
  //  Servers
  // ==========================================================================
  public ServerStateNode getOrCreateServer(final ServerName serverName) {
    ServerStateNode node = serverMap.get(serverName);
    if (node == null) {
      node = new ServerStateNode(serverName);
      ServerStateNode oldNode = serverMap.putIfAbsent(serverName, node);
      node = oldNode != null ? oldNode : node;
    }
    return node;
  }

  public void removeServer(final ServerName serverName) {
    serverMap.remove(serverName);
  }

  protected ServerStateNode getServerNode(final ServerName serverName) {
    return serverMap.get(serverName);
  }

  public double getAverageLoad() {
    int numServers = 0;
    int totalLoad = 0;
    for (ServerStateNode node: serverMap.values()) {
      int regionCount = node.getRegionCount();
      totalLoad++;
      numServers++;
    }
    return numServers == 0 ? 0.0 : (double)totalLoad / (double)numServers;
  }

  public ServerStateNode addRegionToServer(final ServerName serverName,
      final RegionStateNode regionNode) {
    ServerStateNode serverNode = getOrCreateServer(serverName);
    serverNode.addRegion(regionNode);
    return serverNode;
  }

  public ServerStateNode removeRegionFromServer(final ServerName serverName,
      final RegionStateNode regionNode) {
    ServerStateNode serverNode = getOrCreateServer(serverName);
    serverNode.removeRegion(regionNode);
    return serverNode;
  }
}
