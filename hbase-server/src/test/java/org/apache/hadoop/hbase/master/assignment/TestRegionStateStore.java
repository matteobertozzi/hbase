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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestRegionStateStore {
  private static final Log LOG = LogFactory.getLog(TestRegionStateStore.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static RegionStateStore stateStore;
  private static HMaster master;

  private static void setupConf(final Configuration conf) {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);

    master = UTIL.getMiniHBaseCluster().getMaster();
    stateStore = master.getAssignmentManager().getRegionStateStore();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegionUpdate() throws IOException {
    final HRegionInfo hri = new HRegionInfo(TableName.valueOf("testRegionUpdate"));
    final ServerName s1 = ServerName.valueOf("localhost", 1234, 1);
    final ServerName s2 = ServerName.valueOf("localhost", 5678, 2);

    stateStore.updateRegionLocation(hri, State.OPENING, s1, null, -1);
    verifyRegion(hri, State.OPENING, s1, null, -1);

    stateStore.updateRegionLocation(hri, State.OPEN, s1, s1, 1);
    verifyRegion(hri, State.OPEN, s1, s1, 1);

    stateStore.updateRegionLocation(hri, State.CLOSING, s1, s1, -1);
    verifyRegion(hri, State.CLOSING, s1, s1, 1);

    stateStore.updateRegionLocation(hri, State.CLOSED, s1, s1, -1);
    verifyRegion(hri, State.CLOSED, s1, s1, 1);

    stateStore.updateRegionLocation(hri, State.OPENING, s2, s1, -1);
    verifyRegion(hri, State.OPENING, s2, s1, -1);

    stateStore.updateRegionLocation(hri, State.OPEN, s2, s2, 2);
    verifyRegion(hri, State.OPEN, s2, s2, 1);

    stateStore.updateRegionLocation(hri, State.OFFLINE, s2, s2, -1);
    verifyRegion(hri, State.OFFLINE, s2, s2, 1);

    stateStore.deleteRegion(hri);
    verifyNoRegion(hri);
  }

  @Test
  public void testRegionSplit() throws IOException {
    final byte[] splitKey = Bytes.toBytes("a");
    final HRegionInfo parent = new HRegionInfo(TableName.valueOf("testRegionSplit"));
    final HRegionInfo hriA = new HRegionInfo(parent.getTable(), null, splitKey);
    final HRegionInfo hriB = new HRegionInfo(parent.getTable(), splitKey, null);
    final ServerName serverName = ServerName.valueOf("localhost", 1234, 1);

    stateStore.splitRegion(parent, hriA, hriB, serverName);
    verifyNoRegion(parent);
    verifyRegion(hriA, State.OPENING, serverName, serverName, -1);
    verifyRegion(hriB, State.OPENING, serverName, serverName, -1);
  }

  @Test
  public void testRegionMerge() {
    final byte[] splitKey = Bytes.toBytes("a");
    final HRegionInfo merged = new HRegionInfo(TableName.valueOf("testRegionMerge"));
    final HRegionInfo hriA = new HRegionInfo(merged.getTable(), null, splitKey);
    final HRegionInfo hriB = new HRegionInfo(merged.getTable(), splitKey, null);
    final ServerName serverName = ServerName.valueOf("localhost", 1234, 1);
  }

  private void verifyRegion(final HRegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum)
      throws IOException {
    final AtomicInteger count = new AtomicInteger(0);
    stateStore.visitMeta(new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(final HRegionInfo vRegionInfo, final State vState,
          final ServerName vRegionLocation, final ServerName vLastHost, final long vOpenSeqNum) {
        if (regionInfo.equals(vRegionInfo)) {
          LOG.info("CHECK REGION " + regionInfo);
          count.incrementAndGet();
          assertEquals(state, vState);
          assertEquals(lastHost, vLastHost);
          assertEquals(regionLocation, vRegionLocation);
        } else {
          LOG.info("SKIP CHECK REGION " + vRegionInfo);
        }
      }
    });
    assertEquals(1, count.get());
  }

  private void verifyNoRegion(final HRegionInfo regionInfo) throws IOException {
    final AtomicInteger count = new AtomicInteger(0);
    stateStore.visitMeta(new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(final HRegionInfo vRegionInfo, final State vState,
          final ServerName vRegionLocation, final ServerName vLastHost, final long vOpenSeqNum) {
        if (regionInfo.equals(vRegionInfo)) {
          count.incrementAndGet();
        }
      }
    });
    assertEquals(0, count.get());
  }
}
