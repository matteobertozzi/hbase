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
package org.apache.hadoop.hbase.snapshot.exception;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * Thrown when a snapshot could not be created due to a server-side error when taking the snapshot.
 */
@SuppressWarnings("serial")
public class SnapshotCreationException extends HBaseSnapshotException {

  public SnapshotCreationException(String msg, SnapshotDescription desc) {
    super(msg, desc);
  }

  public SnapshotCreationException(String msg, Throwable cause, SnapshotDescription desc) {
    super(msg, cause, desc);
  }

  public SnapshotCreationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public SnapshotCreationException(String msg) {
    super(msg);
  }

  public SnapshotCreationException(Throwable cause, SnapshotDescription desc) {
    super(cause, desc);
  }

  public SnapshotCreationException(Throwable cause) {
    super(cause);
  }
}