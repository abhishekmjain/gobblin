/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;


public interface DagActionStore {
  public static final String NO_JOB_NAME_DEFAULT = "";
  enum DagActionType {
    ENFORCE_JOB_START_DEADLINE, // Enforce job start deadline
    ENFORCE_FLOW_FINISH_DEADLINE, // Enforce flow finish deadline
    KILL, // Kill invoked through API call
    LAUNCH, // Launch new flow execution invoked adhoc or through scheduled trigger
    REEVALUATE, // Re-evaluate what needs to be done upon receipt of a final job status
    RESUME, // Resume flow invoked through API call
  }

  @Data
  @RequiredArgsConstructor
  class DagAction {
    final String flowGroup;
    final String flowName;
    final long flowExecutionId;
    final String jobName;
    final DagActionType dagActionType;

    public static DagAction forFlow(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) {
      return new DagAction(flowGroup, flowName, flowExecutionId, NO_JOB_NAME_DEFAULT, dagActionType);
    }

    public FlowId getFlowId() {
      return new FlowId().setFlowGroup(this.flowGroup).setFlowName(this.flowName);
    }

    /**
     *   Replace flow execution id with agreed upon event time to easily track the flow
     */
    public DagAction updateFlowExecutionId(long eventTimeMillis) {
      return new DagAction(this.getFlowGroup(), this.getFlowName(), eventTimeMillis, this.getJobName(), this.getDagActionType());
    }

    /**
     * Creates and returns a {@link DagNodeId} for this DagAction.
     */
    public DagNodeId getDagNodeId() {
      return new DagNodeId(this.flowGroup, this.flowName, this.flowExecutionId, this.flowGroup, this.jobName);
    }

    /**
     * Creates and returns a {@link DagManager.DagId} for this DagAction.
     */
    public DagManager.DagId getDagId() {
      return new DagManager.DagId(this.flowGroup, this.flowName, this.flowExecutionId);
    }
  }

  @Data
  @RequiredArgsConstructor
  class DagActionLeaseObject {
    final DagAction dagAction;
    final boolean isReminder;
    final long eventTimeMillis;

    /**
     * Creates a lease object for a dagAction and eventTimeMillis representing an original event (isReminder is False)
     */
    public DagActionLeaseObject(DagAction dagAction, long eventTimeMillis) {
      this.dagAction = dagAction;
      this.isReminder = false;
      this.eventTimeMillis = eventTimeMillis;
    }
  }



  /**
   * Check if an action exists in dagAction store by flow group, flow name, flow execution id, and job name.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param jobName job name for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean exists(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType) throws IOException, SQLException;

  /**
   * Check if an action exists in dagAction store by flow group, flow name, and flow execution id, it assumes jobName is
   * empty ("").
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean exists(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) throws IOException, SQLException;

  /** Persist the {@link DagAction} in {@link DagActionStore} for durability */
  default void addDagAction(DagAction dagAction) throws IOException {
    addJobDagAction(
        dagAction.getFlowGroup(),
        dagAction.getFlowName(),
        dagAction.getFlowExecutionId(),
        dagAction.getJobName(),
        dagAction.getDagActionType());
  }

  /**
   * Persist the dag action in {@link DagActionStore} for durability
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param jobName job name for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  void addJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType) throws IOException;

  /**
   * Persist the dag action in {@link DagActionStore} for durability. This method assumes an empty jobName.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  default void addFlowDagAction(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) throws IOException {
    addDagAction(DagAction.forFlow(flowGroup, flowName, flowExecutionId, dagActionType));
  }

  /**
   * delete the dag action from {@link DagActionStore}
   * @param dagAction containing all information needed to identify dag and specific action value
   * @throws IOException
   * @return true if we successfully delete one record, return false if the record does not exist
   */
  boolean deleteDagAction(DagAction dagAction) throws IOException;

  /***
   * Get all {@link DagAction}s from the {@link DagActionStore}.
   * @throws IOException Exception in retrieving {@link DagAction}s.
   */
  Collection<DagAction> getDagActions() throws IOException;

}
