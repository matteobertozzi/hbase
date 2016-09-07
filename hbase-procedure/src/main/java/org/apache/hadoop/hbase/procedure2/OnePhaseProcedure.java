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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.AvlUtil.AvlInsertOrReplace;
import org.apache.hadoop.hbase.util.AvlUtil.AvlKeyComparator;
import org.apache.hadoop.hbase.util.AvlUtil.AvlLinkedNode;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTree;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class OnePhaseProcedure<TEnvironment> extends Procedure<TEnvironment> {
  // TODO (e.g. used by online snapshots)

  protected static abstract class Entry extends AvlLinkedNode<Entry> {
    private boolean isDone = false;
    private int numRetries = 0;
  }

  private Entry pendingEntriesHead = null;
  private Entry entryMapRoot = null;

  @Override
  protected Procedure[] execute(final TEnvironment env) {
    if (pendingEntriesHead != null) {
      //sendRequests()
    }
    return null;
  }

  @Override
  protected void rollback(TEnvironment env) {
  }

  @Override
  protected boolean abort(TEnvironment env) {
    return false;
  }

  @Override
  protected void serializeStateData(final OutputStream stream) throws IOException {
  }

  @Override
  protected void deserializeStateData(final InputStream stream) throws IOException {
  }
}
