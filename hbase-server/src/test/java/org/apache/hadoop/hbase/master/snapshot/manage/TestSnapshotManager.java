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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test basic snapshot manager functionality
 */
@Category(SmallTests.class)
public class TestSnapshotManager {
  private static final Log LOG = LogFactory.getLog(TestSnapshotManager.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  MasterServices services = Mockito.mock(MasterServices.class);
  ZooKeeperWatcher watcher = Mockito.mock(ZooKeeperWatcher.class);
  ExecutorService pool = Mockito.mock(ExecutorService.class);
  MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
  FileSystem fs;
  {
    try {
      fs = UTIL.getTestFileSystem();
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get test filesystem", e);
    }

  }

  private SnapshotManager getNewManager() throws KeeperException {
    Mockito.reset(services, watcher, pool);
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(mfs.getFileSystem()).thenReturn(fs);
    Mockito.when(mfs.getRootDir()).thenReturn(UTIL.getDataTestDir());
    return new SnapshotManager(services, watcher, pool);
  }



  @Test
  public void testInProcess() throws KeeperException, SnapshotCreationException {
    SnapshotManager manager = getNewManager();
    SnapshotHandler handler = Mockito.mock(SnapshotHandler.class);
    assertFalse("Manager is in process when there is no current handler", manager.isTakingSnapshot());
    manager.setSnapshotHandlerForTesting(handler);
    Mockito.when(handler.isFinished()).thenReturn(false);
    assertTrue("Manager isn't in process when handler is running", manager.isTakingSnapshot());
    Mockito.when(handler.isFinished()).thenReturn(true);
    assertFalse("Manager is process when handler isn't running", manager.isTakingSnapshot());
  }

  /**
   * Test that we stop the running distabled table snapshot by passing along an error to the error
   * handler.
   * @throws Exception
   */
  @Test
  public void testStopPropagation() throws Exception {
    // create a new orchestrator and hook up a listener
    SnapshotManager manager = getNewManager();
    FSUtils.setRootDir(UTIL.getConfiguration(), UTIL.getDataTestDir());

    // setup a mock snapshot to run
    String tableName = "some table";
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("testAbort")
        .setTable(tableName).build();
    Server parent = Mockito.mock(Server.class);
    Mockito.when(parent.getConfiguration()).thenReturn(UTIL.getConfiguration());
    TableDescriptors tables = Mockito.mock(TableDescriptors.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tables);
    HTableDescriptor descriptor = Mockito.mock(HTableDescriptor.class);
    Mockito.when(tables.get(Mockito.anyString())).thenReturn(descriptor);
    MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
    Mockito.when(mfs.getFileSystem()).thenReturn(UTIL.getTestFileSystem());
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(services.getConfiguration()).thenReturn(UTIL.getConfiguration());

    // create a new handler that we will check for errors
    manager.snapshotDisabledTable(snapshot, parent);
    // make sure we submitted the handler, but because its mocked, it doesn't run it.
    Mockito.verify(pool, Mockito.times(1)).submit(Mockito.any(DisabledTableSnapshotHandler.class));

    // pass along the stop notification
    manager.stop("stopping for test");
    SnapshotHandler handler = manager.getCurrentSnapshotHandler();
    assertNotNull("Snare didn't receive error notification from snapshot manager.",
      handler.getExceptionIfFailed());
  }
}