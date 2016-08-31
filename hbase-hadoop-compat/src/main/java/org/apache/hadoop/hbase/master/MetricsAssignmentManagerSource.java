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

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsAssignmentManagerSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "AssignmentManager";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master assingment manager.";

  String RIT_COUNT_NAME = "ritCount";
  String RIT_COUNT_OVER_THRESHOLD_NAME = "ritCountOverThreshold";
  String RIT_OLDEST_AGE_NAME = "ritOldestAge";
  String RIT_DURATION_NAME = "ritDuration";

  String OPERATION_COUNT_NAME = "operationCount";
  String ASSIGN_TIME_NAME = "assign";
  String UNASSIGN_TIME_NAME = "unassign";
  String TRANSITION_REPORT_TIME_NAME = "transitionReport";
  String ONLINE_REPORT_TIME_NAME = "onlineReport";

  /**
   * Set the number of regions in transition.
   *
   * @param ritCount count of the regions in transition.
   */
  void setRIT(int ritCount);

  /**
   * Set the count of the number of regions that have been in transition over the threshold time.
   *
   * @param ritCountOverThreshold number of regions in transition for longer than threshold.
   */
  void setRITCountOverThreshold(int ritCountOverThreshold);

  /**
   * Set the oldest region in transition.
   *
   * @param age age of the oldest RIT.
   */
  void setRITOldestAge(long age);

  void updateRitDuration(long duration);

  /**
   * Increment the count of assignment operation (assign/unassign).
   */
  void incrementOperationCounter();

  /**
   * Add the time took to perform the last assign operation
   */
  void updateAssignTime(long time);

  /**
   * Add the time took to perform the last unassign operation
   */
  void updateUnassignTime(long time);

  /**
   * Add the time took to perform the last transition report handling
   */
  void updateTransitionReportTime(long time);

  /*
   * Add the time took to perform the last online report handling
   */
  void updateOnlineReportTime(long time);
}
