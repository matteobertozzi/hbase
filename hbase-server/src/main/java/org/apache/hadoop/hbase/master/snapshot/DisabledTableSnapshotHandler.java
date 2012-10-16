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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.task.ReferenceServerRecoverEditsTask;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotReferences;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;

/**
 * Take a snapshot of a disabled table.
 */
public class DisabledTableSnapshotHandler extends TableSnapshotHandler {
  private static final Log LOG = LogFactory.getLog(DisabledTableSnapshotHandler.class);

  private final Path tdir;
  private final PathFilter visibleDirFilter;
  private final PathFilter fileFilter;
  private final OperationAttemptTimer timer;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param server parent server
   * @param masterServices master services provider
   * @param monitor monitor the health/progress of the snapshot
   * @param manager general manager for the snapshot
   * @throws IOException on unexpected error
   */
  public DisabledTableSnapshotHandler(SnapshotDescription snapshot, Server server,
      final MasterServices masterServices, SnapshotErrorListener monitor, SnapshotManager manager)
      throws IOException {
    super(snapshot, server, masterServices, monitor, manager);
    this.tdir = HTableDescriptor.getTableDir(this.rootDir, this.tableName);
    this.visibleDirFilter = new FSUtils.VisibleDirectory(fs);
    // !directory
    this.fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !visibleDirFilter.accept(path);
      }
    };
    // setup the timer
    timer = TakeSnapshotUtils.getMasterTimerAndBindToMonitor(snapshot, conf,
      monitor);
  }

  // TODO consider parallelizing these operations since they are independent. Right now its just
  // easier to keep them serial though
  @Override
  protected void snapshot(List<HRegionInfo> regions) throws IOException, KeeperException {
    // 0. start the timer for taking the snapshot
    timer.start();

    // 1. write the table info to disk
    LOG.info("Starting to copy tableinfo for offline snapshot:\n" + snapshot);
    TableInfoCopyTask tableInfo = new TableInfoCopyTask(this.monitor, snapshot, fs,
        FSUtils.getRootDir(conf));
    tableInfo.run();
    monitor.failOnError();

    SnapshotReferences.Builder snapshotRefs = SnapshotReferences.newBuilder();

    // 2. for each region, write all the info to disk
    LOG.info("Starting to write region info and WALs for regions for offline snapshot:" + snapshot);
    for (HRegionInfo regionInfo : regions) {
      // 2.1. create references for each of the WAL files for the region
      Path regionDir = new Path(tdir, regionInfo.getEncodedName());
      ReferenceServerRecoverEditsTask op = new ReferenceServerRecoverEditsTask(snapshot,
          this.monitor, regionDir, conf, fs);
      op.run();
      monitor.failOnError();

      // 2.2. reference all region, family, hfiles
      snapshotRefs.addRegions(SnapshotReferenceUtil.createRegionReference(fs, regionDir, regionInfo));
      monitor.failOnError();
    }

    Path rootDir = FSUtils.getRootDir(conf);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    SnapshotReferenceUtil.writeSnapshotReferences(fs, workingDir,
      server.getServerName().toString(), snapshotRefs.build());

    // 5. mark operation as completed
    timer.complete();
  }
}