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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.BaseConfigurable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.cleaner.FileCleanerDelegate;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.DirFilter;

/**
 * Utility class to help clean up parts of a snapshot from the archive directories.
 */
@InterfaceAudience.Private
public class SnapshotCleanerChoreUtil extends BaseConfigurable implements Stoppable,
    FileCleanerDelegate {

  private static final Log LOG = LogFactory.getLog(SnapshotCleanerChoreUtil.class);
  private Timer refreshTimer;
  private volatile boolean stopped = false;
  private Set<String> fileNameCache = new TreeSet<String>();
  private final FileSystem fs;
  private final Path rootDir;
  private final DirFilter dirFilter;
  private final SnapshotCleanerCacheLoader parent;

  public SnapshotCleanerChoreUtil(SnapshotCleanerCacheLoader parent, Configuration conf,
      long cacheRefreshPeriod, String refreshThreadName) throws IOException {
    this.parent = parent;
    super.setConf(conf);
    this.fs = FSUtils.getCurrentFileSystem(this.getConf());
    this.rootDir = FSUtils.getRootDir(this.getConf());
    this.dirFilter = new FSUtils.DirFilter(fs);
    // periodically refresh the hfile cache to make sure we aren't superfluously saving hfiles.
    this.refreshTimer = new Timer(refreshThreadName, true);
    this.refreshTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try {
          SnapshotCleanerChoreUtil.this.refreshCache();
        } catch (IOException e) {
          LOG.warn("Failed to refresh snapshot hfile cache!");
        }
      }
    }, 0, cacheRefreshPeriod);

  }

  @Override
  public synchronized boolean isFileDeletable(Path file) {
    if (Thread.interrupted()) {
      LOG.debug("Found a thread interruption, server is shutting down.");
      this.stop("Found thread was interrupted, means we need to stop.");
    }
    LOG.debug("Checking to see if:" + file + " is deletable");
    String fileName = file.getName();
    if (this.fileNameCache.contains(fileName)) {
      return false;
    }

    // if we don't have it in the cache, the cache may be empty, so we need to
    // refresh it and then check again
    try {
      refreshCache();
    } catch (IOException e) {
      LOG.error("Couldn't refresh HLog cache, by default not going to delete log file.");
      return false;
    }
    // if we still don't have the log, then you can delete it
    return !this.fileNameCache.contains(fileName);
  }

  /**
   * Refresh the current view of the files in a snapshot. Scans all the files in the .snapshot
   * directory, so it can be optimized, but it works for the moment and is a background task, so a
   * little longer is okay.
   */
  private synchronized void refreshCache() throws IOException {
    LOG.debug("Refreshing file cache.");
    this.fileNameCache.clear();

    refreshSnapshotCache(SnapshotDescriptionUtils.getSnapshotRootDir(rootDir));
    refreshSnapshotCache(SnapshotDescriptionUtils.getSnapshotWorkingDir(rootDir));
  }

  private void refreshSnapshotCache(final Path snapshotRoot) throws IOException {
    FileStatus[] snapshots = FSUtils.listStatus(fs, snapshotRoot, dirFilter);
    if (snapshots == null) return;

    try {
      for (FileStatus snapshot : snapshots) {
        if (stopped) throw new IOException("Stopped! Cannot read any more files.");
        parent.loadFiles(this.fs, snapshot, this.fileNameCache);
      }
    } catch (IOException e) {
      LOG.warn("Failed to refresh files cache for snapshotsRoot=" + snapshotRoot, e);
      throw e;
    }
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stopping snapshot file cleaner");
    this.refreshTimer.cancel();
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
