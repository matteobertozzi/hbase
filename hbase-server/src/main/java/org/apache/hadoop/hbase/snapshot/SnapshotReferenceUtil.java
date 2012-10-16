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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.FSUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotReferences;


@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  public interface StoreFilesFilter {
    void storeFile (final HRegionInfo regionInfo, final String family, final String hfile)
      throws IOException;
  }

  public interface RecoveredEditsFilter {
    void recoveredEdits (final String region, final String logfile)
      throws IOException;
  }

  public interface LogFilesFilter {
    void logFile (final String server, final String logfile)
      throws IOException;
  }

  public interface FilesFilter extends StoreFilesFilter, RecoveredEditsFilter, LogFilesFilter {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }

  /**
   * Get the snapshot recovered.edits root dir
   */
  public static Path getRecoveredEditsDir(Path snapshotDir) {
    return new Path(snapshotDir, '.' + HLog.RECOVERED_EDITS_DIR);
  }

  /**
   * Get the snapshot recovered.edits dir for the specified region
   */
  public static Path getRecoveredEditsDir(Path snapshotDir, String regionName) {
    return new Path(getRecoveredEditsDir(snapshotDir), regionName);
  }

  /**
   * Get the snapshot recovered.edits file
   */
  public static Path getRecoveredEdits(Path snapshotDir, String regionName, String logfile) {
    return new Path(getRecoveredEditsDir(snapshotDir, regionName), logfile);
  }

  public static Path getStoreReferencesDir(Path snapshotDir) {
    return new Path(snapshotDir, ".store-refs");
  }

  public static void writeSnapshotReferences(final FileSystem fs, final Path workingDir,
      final String serverName, final SnapshotReferences snapshotRefs) throws IOException {
    Path snapshotInfo = new Path(getStoreReferencesDir(workingDir), serverName);
    try {
      fs.mkdirs(snapshotInfo.getParent());
      FSDataOutputStream out = fs.create(snapshotInfo, true);
      try {
        snapshotRefs.writeTo(out);
      } finally {
        out.close();
      }
    } catch (IOException e) {
      // if we get an exception, try to remove the snapshot info
      if (!fs.delete(snapshotInfo, false)) {
        // if we didn't delete the file, then throw an error
        if (fs.exists(snapshotInfo)) {
          throw new IOException("Couldn't delete snapshot info file: " + snapshotInfo);
        }
      }
    }
  }

  public static SnapshotReferences readSnapshotReferences(final FileSystem fs, final Path storeRefsPath)
      throws IOException {
    FSDataInputStream in = null;
    try {
      in = fs.open(storeRefsPath);
      return SnapshotReferences.parseFrom(in);
    } finally {
      if (in != null) in.close();
    }
  }

  /**
   * Create a snapshot reference for the specified region.
   * Loops through all the families and hfiles and keeps a reference.
   */
  public static SnapshotReferences.Region createRegionReference(final FileSystem fs,
      final Path regionDir, final HRegionInfo regionInfo) throws IOException {
    SnapshotReferences.Region.Builder regionRef = SnapshotReferences.Region.newBuilder();
    regionRef.setRegionInfo(HRegionInfo.convert(regionInfo));
    FileStatus[] families = FSUtils.listStatus(fs, regionDir, new FSUtils.FamilyDirFilter(fs));
    for (FileStatus family: families) {
      regionRef.addFamilies(createFamilyReference(fs, family.getPath()));
    }
    return regionRef.build();
  }

  /**
   * Create a snapshot reference for the specified family.
   * Loops through all the family hfiles on disk and keeps a reference.
   */
  public static SnapshotReferences.Family createFamilyReference(final FileSystem fs,
      final Path familyDir) throws IOException {
    SnapshotReferences.Family.Builder familyRef = SnapshotReferences.Family.newBuilder();
    familyRef.setName(familyDir.getName());
    FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir);
    for (FileStatus hfile: hfiles) {
      familyRef.addStoreFiles(createStoreFileReference(fs, hfile.getPath()));
    }
    return familyRef.build();
  }

  /**
   * Create a snapshot reference for the specified store file.
   * A store file can be a:
   *  - Reference: top/bottom half of hfile with a splitKey
   *  - HFileLink: link to a hfile (typically created by snapshot restore)
   *  - HFile: normal hbase store file.
   */
  public static SnapshotReferences.StoreFile createStoreFileReference(final FileSystem fs,
      final Path storeFile) throws IOException {
    SnapshotReferences.StoreFile.Builder storeRef = SnapshotReferences.StoreFile.newBuilder();
    if (Reference.checkReference(storeFile)) {
      Reference ref = Reference.read(fs, storeFile);
      storeRef.setName(Reference.getDeferencedHFileName(storeFile.getName()));
      storeRef.setReference(ref.convert());
    } else if (HFileLink.isHFileLink(storeFile)) {
      storeRef.setName(HFileLink.getReferencedHFileName(storeFile.getName()));
    } else {
      storeRef.setName(storeFile.getName());
    }
    return storeRef.build();
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   */
  public static void listReferencedFiles(final FileSystem fs, final Path snapshotDir,
      final FilesFilter filter) throws IOException {
    listStoreFiles(fs, snapshotDir, filter);
    listRecoveredEdits(fs, snapshotDir, filter);
    listLogFiles(fs, snapshotDir, filter);
  }

  public static Map<HRegionInfo, SnapshotReferences.Region> getStoreFiles(final FileSystem fs,
      final Path snapshotDir) throws IOException
  {
    FileStatus[] snapshotStoreRefs = FSUtils.listStatus(fs, getStoreReferencesDir(snapshotDir));
    if (snapshotStoreRefs == null) return null;

    Map<HRegionInfo, SnapshotReferences.Region> regionRefs =
      new HashMap<HRegionInfo, SnapshotReferences.Region>();
    for (FileStatus storeRefs: snapshotStoreRefs) {
      SnapshotReferences serverRefs = readSnapshotReferences(fs, storeRefs.getPath());
      for (SnapshotReferences.Region region: serverRefs.getRegionsList()) {
        HRegionInfo regionInfo = HRegionInfo.convert(region.getRegionInfo());
        regionRefs.put(regionInfo, region);
      }
    }
    return regionRefs;
  }

  /**
   * Iterate over the snapshot store files
   */
  public static void listStoreFiles(final FileSystem fs, final Path snapshotDir,
      final StoreFilesFilter filter) throws IOException {
    Map<HRegionInfo, SnapshotReferences.Region> regionRefs = getStoreFiles(fs, snapshotDir);
    if (regionRefs == null) return;

    for (Map.Entry<HRegionInfo, SnapshotReferences.Region> region: regionRefs.entrySet()) {
      listStoreFiles(region.getKey(), region.getValue(), filter);
    }
  }

  public static void listStoreFiles(final HRegionInfo regionInfo,
      final SnapshotReferences.Region region, final StoreFilesFilter filter) throws IOException {
    for (SnapshotReferences.Family family: region.getFamiliesList()) {
      for (SnapshotReferences.StoreFile storeFile: family.getStoreFilesList()) {
        filter.storeFile(regionInfo, family.getName(), storeFile.getName());
      }
    }
  }

  /**
   * Iterate over the snapshot recovered.edits
   */
  public static void listRecoveredEdits(final FileSystem fs, final Path snapshotDir,
      final RecoveredEditsFilter filter) throws IOException {
    Path editsDir = getRecoveredEditsDir(snapshotDir);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, editsDir);
    if (logServerDirs == null) return;

    for (FileStatus serverLogs: logServerDirs) {
      FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
      if (hlogs == null) continue;

      for (FileStatus hlogRef: hlogs) {
        Path hlogRefPath = hlogRef.getPath();
        String logName = hlogRefPath.getName();
        String regionName = hlogRefPath.getParent().getName();
        filter.recoveredEdits(regionName, logName);
      }
    }
  }

  /**
   * Iterate over the snapshot recovered.edits and log files
   */
  public static void listLogFiles(final FileSystem fs, final Path snapshotDir,
      final LogFilesFilter filter) throws IOException {
    Path logsDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, logsDir);
    if (logServerDirs == null) return;

    for (FileStatus serverLogs: logServerDirs) {
      FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
      if (hlogs == null) continue;

      for (FileStatus hlogRef: hlogs) {
        Path hlogRefPath = hlogRef.getPath();
        String logName = hlogRefPath.getName();
        String serverName = hlogRefPath.getParent().getName();
        filter.logFile(serverName, logName);
      }
    }
  }
}
