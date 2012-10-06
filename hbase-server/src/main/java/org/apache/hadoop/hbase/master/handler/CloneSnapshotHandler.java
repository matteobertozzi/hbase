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

package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to Clone a snapshot.
 */
@InterfaceAudience.Private
public class CloneSnapshotHandler extends CreateTableHandler {
  private final SnapshotDescription snapshot;

  public CloneSnapshotHandler(final Server server, final MasterFileSystem fileSystemManager,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor,
      final Configuration conf, final CatalogTracker catalogTracker,
      final AssignmentManager assignmentManager)
      throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    super(server, fileSystemManager, hTableDescriptor, conf, null,
      catalogTracker, assignmentManager);
    this.snapshot = snapshot;
  }

  @Override
  protected List<HRegionInfo> handleCreateRegions() throws IOException, KeeperException {
    byte[] tableName = hTableDescriptor.getName();

    try {
      FileSystem fs = fileSystemManager.getFileSystem();
      Path rootDir = fileSystemManager.getRootDir();
      Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);

      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
                        catalogTracker, snapshot, snapshotDir, hTableDescriptor, tableDir);
      restoreHelper.restore();

      return MetaReader.getTableRegions(catalogTracker, tableName);
    } catch (IOException e) {
      throw new RestoreSnapshotException(e);
    }
  }
}
