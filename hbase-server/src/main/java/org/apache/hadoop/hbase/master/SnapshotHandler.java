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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * Handle the current snapshot under process
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface SnapshotHandler extends Stoppable {

  /**
   * Check to see if the snapshot is finished
   * @return <tt>false</tt> if the snapshot is still in progress, <tt>true</tt> if the snapshot has
   *         finished (regardless of success or not)
   */
  public boolean isFinished();

  /**
   * @return the description of the snapshot being run
   */
  public SnapshotDescription getSnapshot();

  /**
   * Get the exception that caused the snapshot to fail, if the snapshot has failed.
   * @return <tt>null</tt> if the snapshot succeeded, or the {@link HBaseSnapshotException} that
   *         caused the snapshot to fail.
   */
  public HBaseSnapshotException getExceptionIfFailed();

}
