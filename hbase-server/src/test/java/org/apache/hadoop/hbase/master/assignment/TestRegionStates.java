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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
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
public class TestRegionStates {
  private static final Log LOG = LogFactory.getLog(TestRegionStates.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static ThreadPoolExecutor threadPool;
  private static ExecutorCompletionService executorService;

  @BeforeClass
  public static void setUp() throws Exception {
    threadPool = Threads.getBoundedCachedThreadPool(32, 60L, TimeUnit.SECONDS,
      Threads.newDaemonThreadFactory("ProcedureDispatcher",
        new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            LOG.warn("Failed thread " + t.getName(), e);
          }
        }));
    executorService = new ExecutorCompletionService(threadPool);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    threadPool.shutdown();
  }

  @Before
  public void testSetup() {
  }

  @After
  public void testTearDown() throws Exception {
    while (true) {
      Future<Object> f = executorService.poll();
      if (f == null) break;
      f.get();
    }
  }

  private static void waitExecutorService(final int count) throws Exception {
    for (int i = 0; i < count; ++i) {
      executorService.take().get();
    }
  }

  // ==========================================================================
  //  Regions related
  // ==========================================================================

  @Test
  public void testRegionDoubleCreation() throws Exception {
    // NOTE: HRegionInfo sort by table first, so we are relying on that
    final TableName TABLE_NAME_A = TableName.valueOf("testOrderedByTableA");
    final TableName TABLE_NAME_B = TableName.valueOf("testOrderedByTableB");
    final TableName TABLE_NAME_C = TableName.valueOf("testOrderedByTableC");
    final RegionStates stateMap = new RegionStates();
    final int NRUNS = 1000;
    final int NSMALL_RUNS = 3;

    // add some regions for table B
    for (int i = 0; i < NRUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_B, i);
    }
    // re-add the regions for table B
    for (int i = 0; i < NRUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_B, i);
    }
    waitExecutorService(NRUNS * 2);

    // add two other tables A and C that will be placed before and after table B (sort order)
    for (int i = 0; i < NSMALL_RUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_A, i);
      addRegionNode(stateMap, TABLE_NAME_C, i);
    }

    // check for the list of regions of the 3 tables
    checkTableRegions(stateMap, TABLE_NAME_A, NSMALL_RUNS);
    checkTableRegions(stateMap, TABLE_NAME_B, NRUNS);
    checkTableRegions(stateMap, TABLE_NAME_C, NSMALL_RUNS);
  }

  private void checkTableRegions(final RegionStates stateMap,
      final TableName tableName, final int nregions) {
    List<HRegionInfo> hris = stateMap.getRegionsOfTable(tableName);
    assertEquals(nregions, hris.size());
    for (int i = 1; i < hris.size(); ++i) {
      long a = Bytes.toLong(hris.get(i - 1).getStartKey());
      long b = Bytes.toLong(hris.get(i + 0).getStartKey());
      assertEquals(b, a + 1);
    }
  }

  private void addRegionNode(final RegionStates stateMap,
      final TableName tableName, final long regionId) {
    executorService.submit(new Callable<Object>() {
      @Override
      public Object call() {
        HRegionInfo hri = new HRegionInfo(tableName,
          Bytes.toBytes(regionId), Bytes.toBytes(regionId + 1), false, 0);
        return stateMap.getOrCreateRegionNode(hri);
      }
    });
  }

  private Object createRegionNode(final RegionStates stateMap,
      final TableName tableName, final long regionId) {
    return stateMap.getOrCreateRegionNode(createRegionInfo(tableName, regionId));
  }

  private HRegionInfo createRegionInfo(final TableName tableName, final long regionId) {
    return new HRegionInfo(tableName,
      Bytes.toBytes(regionId), Bytes.toBytes(regionId + 1), false, regionId);
  }

  @Test
  public void testGetAssignmentsByTable() {
    testGetAssignmentsByTable(false);
  }

  @Test
  public void testGetAssignmentsByTableForcedByCluster() {
    testGetAssignmentsByTable(true);
  }

  private void testGetAssignmentsByTable(final boolean forcedByCluster) {
    final int NREGIONS_PER_TABLE = 3;
    final TableName[] tables = new TableName[3];
    final ServerName[] servers = new ServerName[3];

    final RegionStates stateMap = new RegionStates();
    final Map<ServerName, Set<HRegionInfo>> mapping = new HashMap<ServerName, Set<HRegionInfo>>();

    for (int i = 0; i < servers.length; ++i) {
      servers[i] = ServerName.valueOf("localhost", 12345 + i, 100);
      mapping.put(servers[i], new HashSet<HRegionInfo>());
    }

    // try to add regions to servers
    for (int i = 0; i < tables.length; ++i) {
      tables[i] = TableName.valueOf("testGetAssignmentsByTable-" + i);
      for (int j = 0; j < NREGIONS_PER_TABLE; ++j) {
        HRegionInfo hri = createRegionInfo(tables[i], j);
        ServerName serverName = servers[((i * NREGIONS_PER_TABLE) + j) % servers.length];

        RegionStateNode regionNode = stateMap.getOrCreateRegionNode(hri);
        regionNode.setRegionLocation(serverName);
        stateMap.addRegionToServer(serverName, regionNode);
        mapping.get(serverName).add(hri);
      }
    }

    final Map<TableName, Map<ServerName, List<HRegionInfo>>> assignmentMap =
        stateMap.getAssignmentsByTable(forcedByCluster);
    if (forcedByCluster) {
      // only one table ENSEMBLE_TABLE_NAME
      assertEquals(1, assignmentMap.size());
      Map<ServerName, List<HRegionInfo>> ensembleMapping = assignmentMap.get(
        HConstants.ENSEMBLE_TABLE_NAME);
      assertTrue(ensembleMapping != null);
      for (int i = 0; i < servers.length; ++i) {
        Set<HRegionInfo> regions = new HashSet<HRegionInfo>(ensembleMapping.get(servers[i]));
        assertEquals(regions, mapping.get(servers[i]));
      }
    } else {
      // regions/server mapping is divided by tables
      assertEquals(tables.length, assignmentMap.size());
      for (int i = 0; i < tables.length; ++i) {
        Map<ServerName, List<HRegionInfo>> tableMapping = assignmentMap.get(tables[i]);
        assertTrue(tableMapping != null);
        for (int j = 0; j < servers.length; ++j) {
          List<HRegionInfo> regions = tableMapping.get(servers[j]);
          assertTrue(regions != null);
          for (HRegionInfo hri: mapping.get(servers[j])) {
            if (!hri.getTable().equals(tables[i])) continue;
            assertTrue(hri.toString(), regions.contains(hri));
          }
        }
      }
    }
  }

  // ==========================================================================
  //  Region Perf related
  // ==========================================================================
  @Test
  public void testPerf() throws Exception {
    final TableName TABLE_NAME = TableName.valueOf("testPerf");
    final int NRUNS = 1000000; // 1M
    final RegionStates stateMap = new RegionStates();

    long st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      final int regionId = i;
      executorService.submit(new Callable<Object>() {
        @Override
        public Object call() {
          HRegionInfo hri = createRegionInfo(TABLE_NAME, regionId);
          return stateMap.getOrCreateRegionNode(hri);
        }
      });
    }
    waitExecutorService(NRUNS);
    long et = System.currentTimeMillis();
    LOG.info(String.format("PERF STATEMAP INSERT: %s %s/sec",
      StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));

    st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      final int regionId = i;
      executorService.submit(new Callable<Object>() {
        @Override
        public Object call() {
          HRegionInfo hri = createRegionInfo(TABLE_NAME, regionId);
          return stateMap.getRegionState(hri);
        }
      });
    }

    waitExecutorService(NRUNS);
    et = System.currentTimeMillis();
    LOG.info(String.format("PERF STATEMAP GET: %s %s/sec",
      StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));
  }

  @Test
  public void testPerfSingleThread() {
    final TableName TABLE_NAME = TableName.valueOf("testPerf");
    final int NRUNS = 1 * 1000000; // 1M

    final RegionStates stateMap = new RegionStates();
    long st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      stateMap.createRegionNode(createRegionInfo(TABLE_NAME, i));
    }
    long et = System.currentTimeMillis();
    LOG.info(String.format("PERF SingleThread: %s %s/sec",
        StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));
  }

  // ==========================================================================
  //  Server related
  // ==========================================================================
}