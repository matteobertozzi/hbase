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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the {@link SnapshotDescription} helper is helping correctly.
 */
@Category(SmallTests.class)
public class TestSnapshotDescriptionUtils {

  private static final Log LOG = LogFactory.getLog(TestSnapshotDescriptionUtils.class);

  @Test
  public void testValidateDescriptor() {
    EnvironmentEdge edge = new EnvironmentEdge() {
      @Override
      public long currentTimeMillis() {
        return 0;
      }
    };
    EnvironmentEdgeManagerTestHelper.injectEdge(edge);

    // check a basic snapshot description
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    builder.setName("snapshot");
    builder.setTable("table");

    // check that time is to an amount in the future
    Configuration conf = new Configuration(false);
    conf.setLong(SnapshotDescriptionUtils.TIMESTAMP_SNAPSHOT_SPLIT_POINT_ADDITION, 1);
    SnapshotDescription desc = SnapshotDescriptionUtils.validate(builder.build(), conf);
    assertEquals("Description creation time wasn't set correctly", 1, desc.getCreationTime());

    // test a global snapshot
    edge = new EnvironmentEdge() {
      @Override
      public long currentTimeMillis() {
        return 2;
      }
    };
    EnvironmentEdgeManagerTestHelper.injectEdge(edge);
    builder.setType(Type.GLOBAL);
    desc = SnapshotDescriptionUtils.validate(builder.build(), conf);
    assertEquals("Description creation time wasn't set correctly", 2, desc.getCreationTime());

    // test that we don't override a given value
    builder.setCreationTime(10);
    desc = SnapshotDescriptionUtils.validate(builder.build(), conf);
    assertEquals("Description creation time wasn't set correctly", 10, desc.getCreationTime());

    try {
      SnapshotDescriptionUtils.validate(SnapshotDescription.newBuilder().setName("fail").build(),
        conf);
      fail("Snapshot was considered valid without a table name");
    } catch (IllegalArgumentException e) {
      LOG.debug("Correctly failed when snapshot doesn't have a tablename");
    }
  }
}