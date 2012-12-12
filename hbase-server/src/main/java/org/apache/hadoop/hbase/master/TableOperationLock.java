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

package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.FSProtos;

/**
 * A callable object that invokes the corresponding action that needs to be
 * taken for unassignment of a region in transition. Implementing as future
 * callable we are able to act on the timeout asynchronously.
 */
@InterfaceAudience.Private
public class TableOperationLock {
  public static final String TABLE_OPERATION_LOCK_FILE = ".operation.lock";

  public enum Type {
    CREATE_TABLE,
    DELETE_TABLE,
    RESTORE_TABLE,
    CLONE_TABLE,
  }

  private final Type type;
  private final String source;

  public TableOperationLock(final Type type) {
    this(type, null);
  }

  public TableOperationLock(final Type type, final String source) {
    this.type = type;
    this.source = source;
  }

  public Type getType() {
    return this.type;
  }

  public String getSource() {
    return this.source;
  }

  public String toString() {
    if (this.source == null) {
      return this.type.name();
    } else {
      return this.type.name() + " source=" + this.source;
    }
  }

  /**
   * Write the Table Operation Lock on disk
   * @parms fs {@link FileSystem} to store the lock file
   * @parms path {@link Path} of the operation lock file
   * @throws IOException if the file cannot be written
   */
  public void write(final FileSystem fs, final Path path) throws IOException {
    FSProtos.TableOperationLock.Builder builder = FSProtos.TableOperationLock.newBuilder();
    if (this.source != null) builder.setSource(this.source);
    switch (type) {
      case CREATE_TABLE:
        builder.setType(FSProtos.TableOperationLock.Type.CREATE_TABLE);
        break;
      case DELETE_TABLE:
        builder.setType(FSProtos.TableOperationLock.Type.DELETE_TABLE);
        break;
      case RESTORE_TABLE:
        builder.setType(FSProtos.TableOperationLock.Type.RESTORE_TABLE);
        break;
      case CLONE_TABLE:
        builder.setType(FSProtos.TableOperationLock.Type.CLONE_TABLE);
        break;
    }

    FSDataOutputStream stream = fs.create(path);
    try {
      builder.build().writeTo(stream);
    } finally {
      stream.close();
    }
  }

  /**
   * Read the Table Operation Lock
   * @parms fs {@link FileSystem} of that contains the lock file
   * @parms path {@link Path} of the operation lock file
   * @return the operation lock object
   * @throws IOException if the file cannot be read
   */
  public static TableOperationLock read(final FileSystem fs, final Path path)
      throws IOException {
    FSDataInputStream stream = fs.open(path);
    try {
      TableOperationLock operationLock = null;

      FSProtos.TableOperationLock proto = FSProtos.TableOperationLock.parseFrom(stream);
      Type type = null;
      switch (proto.getType()) {
        case CREATE_TABLE:
          type = Type.CREATE_TABLE;
          break;
        case DELETE_TABLE:
          type = Type.DELETE_TABLE;
          break;
        case RESTORE_TABLE:
          type = Type.RESTORE_TABLE;
          break;
        case CLONE_TABLE:
          type = Type.CLONE_TABLE;
          break;
        default:
          throw new IllegalArgumentException("Invalid lock operation");
      }

      if (proto.hasSource()) {
        return new TableOperationLock(type, proto.getSource());
      } else {
        return new TableOperationLock(type);
      }
    } finally {
      stream.close();
    }
  }

  /**
   * Returns the path of the table operation lock
   * @parms tableDir {@link Path} of the table
   * @return path of the operation lock file
   */
  public static Path getTableOperationLockFile(final Path tableDir) {
    return new Path(tableDir, TableOperationLock.TABLE_OPERATION_LOCK_FILE);
  }
}
