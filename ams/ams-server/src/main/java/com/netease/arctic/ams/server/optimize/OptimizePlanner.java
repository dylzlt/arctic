/*
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

package com.netease.arctic.ams.server.optimize;

import com.google.common.base.Preconditions;
import com.netease.arctic.ams.api.OptimizeTaskId;
import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.api.properties.OptimizeTaskProperties;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.FilesStatistics;
import com.netease.arctic.ams.server.model.TaskConfig;
import com.netease.arctic.ams.server.utils.FilesStatisticsBuilder;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.CompatiblePropertyUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public abstract class OptimizePlanner<B, I, D, PD, G> implements Comparable<OptimizePlanner> {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizePlanner.class);

  protected final ArcticTable arcticTable;

  // The file info in arctic table
  protected final List<B> baseFiles;
  protected final List<I> insertFiles;
  protected final List<D> eqDeleteFiles;
  protected final List<PD> posDeleteFiles;
  protected final List<G> fileGroups;

  // The same plan group in task means these tasks were planned together and used to calculate quota.
  protected final String planGroup = UUID.randomUUID().toString();
  // We store current partitions in this optimize planner. After plan files, current partitions will be set.
  protected final Set<String> currentPartitions = new HashSet<>();

  protected final int queueId;
  protected final int priority;

  public OptimizePlanner(ArcticTable arcticTable,
                         List<B> baseFiles,
                         List<I> insertFiles,
                         List<D> eqDeleteFiles,
                         List<PD> posDeleteFiles,
                         int queueId,
                         int priority) {
    this(arcticTable, baseFiles, insertFiles, eqDeleteFiles, posDeleteFiles, Collections.emptyList(), queueId, priority);
  }

  public OptimizePlanner(ArcticTable arcticTable,
                         List<G> fileGroups,
                         int queueId,
                         int priority) {
    this(arcticTable, Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), fileGroups, queueId, priority);
  }


  public OptimizePlanner(ArcticTable arcticTable,
                         List<B> baseFiles,
                         List<I> insertFiles,
                         List<D> eqDeleteFiles,
                         List<PD> posDeleteFiles,
                         List<G> fileGroups,
                         int queueId,
                         int priority) {
    Preconditions.checkArgument(priority >= 0, "the priority of optimize planner can't be negative");
    this.arcticTable = arcticTable;
    this.baseFiles = baseFiles;
    this.insertFiles = insertFiles;
    this.eqDeleteFiles = eqDeleteFiles;
    this.posDeleteFiles = posDeleteFiles;
    this.fileGroups = fileGroups;
    this.queueId = queueId;
    this.priority = priority;
  }

  /**
   * Produce the optimizing tasks
   * @return optimizing tasks
   */
  public List<BaseOptimizeTask> plan() {
    long startTime = System.currentTimeMillis();

    // check whether this table can skip plan
    if (!tableNeedPlan()) {
      LOG.debug("{} === skip {} plan, because table don't need plan", tableId(), optimizeType());
      return Collections.emptyList();
    }
    if (!filesNeedPlan()) {
      LOG.debug("{} === skip {} plan, because the files in table don't need plan", tableId(), optimizeType());
      return Collections.emptyList();
    }

    // collect optimize tasks
    List<BaseOptimizeTask> results = collectTasks();

    LOG.debug("{} === {} plan {} tasks and cost {} ms", tableId(), optimizeType(),
        results.size(), System.currentTimeMillis() - startTime);
    return results;
  }

  /**
   * The arctic table id of the optimizing table
   * @return arctic table id
   */
  protected TableIdentifier tableId() {
    return arcticTable.id();
  }

  /**
   * Check whether the table need to plan for the optimizing planner
   * @return {@code true} if this table meet the conditions for planning. else {@code false}.
   */
  abstract boolean tableNeedPlan();

  /**
   * Check whether the files in the table need to plan for the optimizing planner
   * @return {@code true} if this files meet the conditions for planning. else {@code false}.
   */
  abstract boolean filesNeedPlan();

  /**
   * Check whether the partition need to plan for the optimizing planner
   *
   * @param partitionToPath target partition
   * @return {@code true} if this partition meet the conditions for planning. else {@code false}.
   */
  abstract boolean partitionNeedPlan(String partitionToPath);

  /**
   * Collect the optimizing tasks in table level
   * @return the optimizing tasks in table level
   */
  protected List<BaseOptimizeTask> collectTasks() {
    List<BaseOptimizeTask> results = new ArrayList<>();

    List<String> skippedPartitions = new ArrayList<>();
    for (String partitionToPath : partitionPaths()) {
      // partition don't need to plan
      if (!partitionNeedPlan(partitionToPath)) {
        skippedPartitions.add(partitionToPath);
        continue;
      }

      List<BaseOptimizeTask> optimizeTasks = collectTask(partitionToPath);
      LOG.debug("{} partition {} ==== collect {} {} tasks", tableId(), partitionToPath, optimizeTasks.size(),
          optimizeType());
      results.addAll(optimizeTasks);
    }

    LOG.debug("{} ==== after collect {} task, skip partitions {}/{}", tableId(), optimizeType(),
        skippedPartitions.size(), partitionPaths().size());
    return results;
  }

  /**
   * Collect the optimizing tasks in partition level
   * @return the optimizing tasks in partition level
   */
  abstract List<BaseOptimizeTask> collectTask(String partitionToPath);

  /**
   * The partition paths that execute optimize plan
   * @return partition paths
   */
  Set<String> partitionPaths() {
    return this.currentPartitions;
  }

  /**
   * The optimizing type of the optimizing table
   * @return @return {@link OptimizeType}
   */
  abstract OptimizeType optimizeType();

  /**
   * The higher the priority, the smaller the number, but can't be negative.
   * @return the priority of the optimizing planner.
   */
  Integer priority() {
    return this.priority;
  }

  protected void fillOptimizeTask(BaseOptimizeTask optimizeTask,
                                  TaskConfig taskConfig,
                                  FilesStatisticsBuilder baseFb,
                                  FilesStatisticsBuilder insertFb,
                                  FilesStatisticsBuilder deleteFb,
                                  FilesStatisticsBuilder posDeleteFb) {
    FilesStatistics baseFs = baseFb.build();
    FilesStatistics insertFs = insertFb.build();
    FilesStatistics deleteFs = deleteFb.build();
    FilesStatistics posDeleteFs = posDeleteFb.build();

    // file size
    optimizeTask.setBaseFileSize(baseFs.getTotalSize());
    optimizeTask.setInsertFileSize(insertFs.getTotalSize());
    optimizeTask.setDeleteFileSize(deleteFs.getTotalSize());
    optimizeTask.setPosDeleteFileSize(posDeleteFs.getTotalSize());

    // file count
    optimizeTask.setBaseFileCnt(baseFs.getFileCnt());
    optimizeTask.setInsertFileCnt(insertFs.getFileCnt());
    optimizeTask.setDeleteFileCnt(deleteFs.getFileCnt());
    optimizeTask.setPosDeleteFileCnt(posDeleteFs.getFileCnt());

    optimizeTask.setTaskCommitGroup(taskConfig.getCommitGroup());
    optimizeTask.setTaskPlanGroup(taskConfig.getPlanGroup());
    optimizeTask.setCreateTime(taskConfig.getCreateTime());
    optimizeTask.setPartition(taskConfig.getPartition());
    optimizeTask.setQueueId(queueId);
    optimizeTask.setTaskId(new OptimizeTaskId(taskConfig.getOptimizeType(), UUID.randomUUID().toString()));
    optimizeTask.setTableIdentifier(arcticTable.id().buildTableIdentifier());

    Map<String, String> properties = new HashMap<>();
    properties.put(OptimizeTaskProperties.ALL_FILE_COUNT, (optimizeTask.getBaseFiles().size() +
        optimizeTask.getInsertFiles().size() + optimizeTask.getDeleteFiles().size()) +
        optimizeTask.getPosDeleteFiles().size() + "");
    optimizeTask.setProperties(properties);
  }

  protected long smallFileSize() {
    Map<String, String> properties = arcticTable.properties();
    if (!properties.containsKey(TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO) &&
        properties.containsKey(TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD)) {
      return Long.parseLong(properties.get(TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD));
    } else {
      long targetSize = PropertyUtil.propertyAsLong(properties, TableProperties.SELF_OPTIMIZING_TARGET_SIZE,
          TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT);
      int fragmentRatio = PropertyUtil.propertyAsInt(properties, TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO,
          TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO_DEFAULT);
      return targetSize / fragmentRatio;
    }
  }

  protected long smallFileCountThreshold() {
    return  CompatiblePropertyUtil.propertyAsInt(arcticTable.properties(),
        TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT,
        TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT_DEFAULT);
  }

  protected long targetSize() {
    return PropertyUtil.propertyAsLong(arcticTable.properties(),
        TableProperties.SELF_OPTIMIZING_TARGET_SIZE,
        TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT);
  }

  protected double duplicateRatio() {
    return PropertyUtil.propertyAsDouble(arcticTable.properties(),
        TableProperties.SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO,
        TableProperties.SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO_DEFAULT);
  }

  @Override
  public int compareTo(@NotNull OptimizePlanner o) {
    return this.priority().compareTo(o.priority());
  }
}
