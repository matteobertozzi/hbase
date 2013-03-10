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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.ArrayList;

import com.google.protobuf.ByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

/**
 * Utility methods for interacting with the snapshot referenced files.
 */
@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  public static final Log LOG = LogFactory.getLog(SnapshotReferenceUtil.class);

  private static final String REGION_MANIFEST_NAME = "region-manifest.";

  public interface StoreFileVisitor {
    void storeFile(final HRegionInfo regionInfo, final String familyName,
       final SnapshotRegionManifest.StoreFile storeFile) throws IOException;
  }

  public interface FileVisitor extends StoreFileVisitor,
    FSVisitor.RecoveredEditsVisitor, FSVisitor.LogFileVisitor {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }


  public static SnapshotRegionManifest buildManifestFromRegion (final HRegion region) {
    SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();

    // 1. dump region meta info into the snapshot directory
    LOG.debug("Storing region-info for snapshot.");
    manifest.setRegionInfo(HRegionInfo.convert(region.getRegionInfo()));

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for hfiles");

    // This ensures that we have an atomic view of the directory as long as we have < ls limit
    // (batch size of the files in a directory) on the namenode. Otherwise, we get back the files in
    // batches and may miss files being added/deleted. This could be more robust (iteratively
    // checking to see if we have all the files until we are sure), but the limit is currently 1000
    // files/batch, far more than the number of store files under a single column family.
    for (Store store : region.getStores().values()) {
      // 2.1. build the snapshot reference for the store
      SnapshotRegionManifest.FamilyFiles.Builder family =
            SnapshotRegionManifest.FamilyFiles.newBuilder();
      family.setFamilyName(ByteString.copyFrom(store.getFamily().getName()));

      List<StoreFile> storeFiles = new ArrayList<StoreFile>(store.getStorefiles());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding snapshot references for " + storeFiles  + " hfiles");
      }

      // 2.2. iterate through all the store's files and create "references".
      for (int i = 0, sz = storeFiles.size(); i < sz; i++) {
        StoreFile storeFile = storeFiles.get(i);

        // create "reference" to this store file.
        LOG.debug("Creating reference for file (" + (i+1) + "/" + sz + "): " + storeFile.getPath());
        SnapshotRegionManifest.StoreFile.Builder sfManifest =
              SnapshotRegionManifest.StoreFile.newBuilder();
        sfManifest.setName(storeFile.getPath().getName());
        if (storeFile.isReference()) {
          sfManifest.setReference(storeFile.getReference().convert());
        }
        family.addStoreFiles(sfManifest.build());
      }

      manifest.addFamilyFiles(family.build());
    }

    return manifest.build();
  }

  public static SnapshotRegionManifest buildManifestFromDisk (final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs,
          tableDir, regionInfo);
    SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();

    // 1. dump region meta info into the snapshot directory
    LOG.debug("Storing region-info for snapshot.");
    manifest.setRegionInfo(HRegionInfo.convert(regionInfo));

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for hfiles");

    // This ensures that we have an atomic view of the directory as long as we have < ls limit
    // (batch size of the files in a directory) on the namenode. Otherwise, we get back the files in
    // batches and may miss files being added/deleted. This could be more robust (iteratively
    // checking to see if we have all the files until we are sure), but the limit is currently 1000
    // files/batch, far more than the number of store files under a single column family.
    Collection<String> familyNames = regionFs.getFamilies();
    if (familyNames != null) {
      for (String familyName: familyNames) {
        Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(familyName);
        if (storeFiles == null) continue;

        // 2.1. build the snapshot reference for the store
        SnapshotRegionManifest.FamilyFiles.Builder family =
              SnapshotRegionManifest.FamilyFiles.newBuilder();
        family.setFamilyName(ByteString.copyFrom(Bytes.toBytes(familyName)));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding snapshot references for " + storeFiles  + " hfiles");
        }

        // 2.2. iterate through all the store's files and create "references".
        int i = 0;
        int sz = storeFiles.size();
        for (StoreFileInfo storeFile: storeFiles) {
          // create "reference" to this store file.
          LOG.debug("Creating reference for file ("+ (++i) +"/" + sz + "): " + storeFile.getPath());
          SnapshotRegionManifest.StoreFile.Builder sfManifest =
                SnapshotRegionManifest.StoreFile.newBuilder();
          sfManifest.setName(storeFile.getPath().getName());
          if (storeFile.isReference()) {
            sfManifest.setReference(storeFile.getReference().convert());
          }
          family.addStoreFiles(sfManifest.build());
        }

        manifest.addFamilyFiles(family.build());
      }
    }

    return manifest.build();
  }

  public static List<SnapshotRegionManifest> loadRegionsManifest (final FileSystem fs,
      final Path snapshotDir) throws IOException {
    FileStatus[] manifestFiles = FSUtils.listStatus(fs, snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(REGION_MANIFEST_NAME);
      }
    });

    if (manifestFiles == null || manifestFiles.length == 0) return null;

    ArrayList<SnapshotRegionManifest> manifests =
        new ArrayList<SnapshotRegionManifest>(manifestFiles.length);
    for (FileStatus st: manifestFiles) {
      FSDataInputStream stream = fs.open(st.getPath());
      try {
        manifests.add(SnapshotRegionManifest.parseFrom(stream));
      } finally {
        stream.close();
      }
    }

    return manifests;
  }

  public static void writeManifest (final FileSystem fs, final Path snapshotDir,
      final String name, final SnapshotRegionManifest manifest) throws IOException {
    Path manifestPath = new Path(snapshotDir, REGION_MANIFEST_NAME + '.' + name);
    FSDataOutputStream stream = fs.create(manifestPath);
    try {
      manifest.writeDelimitedTo(stream);
    } finally {
      stream.close();
    }
  }

  /**
   * Get log directory for a server in a snapshot.
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }

  /**
   * Get the snapshotted recovered.edits dir for the specified region.
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param regionName name of the region
   * @return path to the recovered.edits directory for the specified region files.
   */
  public static Path getRecoveredEditsDir(Path snapshotDir, String regionName) {
    return HLogUtil.getRegionDirRecoveredEditsDir(new Path(snapshotDir, regionName));
  }

  /**
   * Get the snapshot recovered.edits file
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param regionName name of the region
   * @param logfile name of the edit file
   * @return full path of the log file for the specified region files.
   */
  public static Path getRecoveredEdits(Path snapshotDir, String regionName, String logfile) {
    return new Path(getRecoveredEditsDir(snapshotDir, regionName), logfile);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final FileSystem fs, final Path snapshotDir,
      final FileVisitor visitor) throws IOException {
    visitTableStoreFiles(fs, snapshotDir, visitor);
    visitRecoveredEdits(fs, snapshotDir, visitor);
    visitLogFiles(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot store files
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitTableStoreFiles(final FileSystem fs, final Path snapshotDir,
      final StoreFileVisitor visitor) throws IOException {
    List<SnapshotRegionManifest> regionManifests = loadRegionsManifest(fs, snapshotDir);
    if (regionManifests == null || regionManifests.size() == 0) return;
  }

  /**
   * Iterate over the snapshot recovered.edits
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the recovered.edits files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRecoveredEdits(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.RecoveredEditsVisitor visitor) throws IOException {
    FSVisitor.visitTableRecoveredEdits(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot log files
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the log files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitLogFiles(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.LogFileVisitor visitor) throws IOException {
    FSVisitor.visitLogFiles(fs, snapshotDir, visitor);
  }

  /**
   * Returns the store file names in the snapshot.
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  public static Set<String> getHFileNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    final Set<String> names = new HashSet<String>();
    visitTableStoreFiles(fs, snapshotDir, new StoreFileVisitor() {
      public void storeFile (final HRegionInfo regionInfo, final String family,
          final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        String hfile = storeFile.getName();
        if (HFileLink.isHFileLink(hfile)) {
          names.add(HFileLink.getReferencedHFileName(hfile));
        } else {
          names.add(hfile);
        }
      }
    });
    return names;
  }

  /**
   * Returns the log file names available in the snapshot.
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hlogs in the specified snaphot
   */
  public static Set<String> getHLogNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    final Set<String> names = new HashSet<String>();
    visitLogFiles(fs, snapshotDir, new FSVisitor.LogFileVisitor() {
      public void logFile (final String server, final String logfile) throws IOException {
        names.add(logfile);
      }
    });
    return names;
  }
}
