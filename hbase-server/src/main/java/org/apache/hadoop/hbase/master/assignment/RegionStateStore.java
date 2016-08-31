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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.util.MultiHConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class RegionStateStore {
  private static final Log LOG = LogFactory.getLog(RegionStateStore.class);

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

  private final MasterServices master;

  private MultiHConnection multiHConnection;

  public RegionStateStore(final MasterServices master) {
    this.master = master;
  }

  public void start() throws IOException {
  }

  public void stop() {
    if (multiHConnection != null) {
      multiHConnection.close();
      multiHConnection = null;
    }
  }

  public void updateRegionLocation(final HRegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum)
      throws IOException {
    if (regionInfo.isMetaRegion()) {
      updateMetaLocation(regionInfo, regionLocation);
    } else {
      updateUserRegionLocation(regionInfo, state, regionLocation, lastHost, openSeqNum);
    }
  }

  public void updateRegionState(final long openSeqNum, final RegionState newState,
      final RegionState oldState) throws IOException {
    updateRegionLocation(newState.getRegion(), newState.getState(), newState.getServerName(),
        oldState != null ? oldState.getServerName() : null, openSeqNum);
  }

  protected void updateMetaLocation(final HRegionInfo regionInfo, final ServerName serverName)
      throws IOException {
    try {
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName,
        regionInfo.getReplicaId(), State.OPEN);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  protected void updateUserRegionLocation(final HRegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum)
      throws IOException {
    final int replicaId = regionInfo.getReplicaId();
    final Put put = new Put(MetaTableAccessor.getMetaKeyForRegion(regionInfo));
    final StringBuilder info = new StringBuilder("Updating hbase:meta row ");
    info.append(regionInfo.getRegionNameAsString()).append(" with state=").append(state);
    if (openSeqNum >= 0) {
      Preconditions.checkArgument(state == State.OPEN && regionLocation != null,
          "Open region should be on a server");
      MetaTableAccessor.addLocation(put, regionLocation, openSeqNum, -1, replicaId);
      info.append(", openSeqNum=").append(openSeqNum);
      info.append(", server=").append(regionLocation);
    } else if (regionLocation != null && !regionLocation.equals(lastHost)) {
      put.addImmutable(HConstants.CATALOG_FAMILY, getServerNameColumn(replicaId),
          Bytes.toBytes(regionLocation.getServerName()));
      info.append(", sn=").append(regionLocation);
    }
    put.addImmutable(HConstants.CATALOG_FAMILY, getStateColumn(replicaId),
      Bytes.toBytes(state.name()));
    LOG.info(info);

    final HTableDescriptor htd = master.getTableDescriptors().get(regionInfo.getTable());
    final boolean serialReplication = (htd != null) ? htd.hasSerialReplicationScope() : false;
    if (serialReplication && state == State.OPEN) {
      Put barrierPut = MetaTableAccessor.makeBarrierPut(regionInfo.getEncodedNameAsBytes(),
          openSeqNum, regionInfo.getTable().getName());
      updateRegionLocation(regionInfo, state, put, barrierPut);
    } else {
      updateRegionLocation(regionInfo, state, put);
    }
  }

  protected void updateRegionLocation(final HRegionInfo regionInfo, final State state,
      final Put... put) throws IOException {
    synchronized (this) {
      if (multiHConnection == null) {
        multiHConnection = new MultiHConnection(master.getConfiguration(), 1);
      }
    }

    try {
      multiHConnection.processBatchCallback(Arrays.asList(put), TableName.META_TABLE_NAME, null, null);
    } catch (IOException e) {
      String msg = String.format("Failed to persist region=%s state=%s",
        regionInfo.getShortNameToLog(), state);
      LOG.error(msg, e);
      master.abort(msg, e);
      throw e;
    }
  }

  // ==========================================================================
  //  Server Name
  // ==========================================================================

  /**
   * Returns the {@link ServerName} from catalog table {@link Result}
   * where the region is transitioning. It should be the same as
   * {@link MetaTableAccessor#getServerName(Result,int)} if the server is at OPEN state.
   * @param r Result to pull the transitioning server name from
   * @return A ServerName instance or {@link MetaTableAccessor#getServerName(Result,int)}
   * if necessary fields not found or empty.
   */
  static ServerName getRegionServer(final Result r, int replicaId) {
    final Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
        getServerNameColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) {
      RegionLocations locations = MetaTableAccessor.getRegionLocations(r);
      if (locations != null) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (location != null) {
          return location.getServerName();
        }
      }
      return null;
    }
    return ServerName.parseServerName(Bytes.toString(cell.getValueArray(),
      cell.getValueOffset(), cell.getValueLength()));
  }

  private static long cellToLong(final Cell cell) {
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  private static String cellToString(final Cell cell) {
    return Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  private static byte[] getServerNameColumn(int replicaId) {
    return replicaId == 0
        ? HConstants.SERVERNAME_QUALIFIER
        : Bytes.toBytes(HConstants.SERVERNAME_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
          + String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  // ==========================================================================
  //  Region State
  // ==========================================================================

  /**
   * Pull the region state from a catalog table {@link Result}.
   * @param r Result to pull the region state from
   * @return the region state, or OPEN if there's no value written.
   */
  protected State getRegionState(final Result r, int replicaId) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, getStateColumn(replicaId));
    if (cell == null || cell.getValueLength() == 0) return State.OPENING;
    return State.valueOf(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
  }

  private static byte[] getStateColumn(int replicaId) {
    return replicaId == 0
        ? HConstants.STATE_QUALIFIER
        : Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
          + String.format(HRegionInfo.REPLICA_ID_FORMAT, replicaId));
  }
}
