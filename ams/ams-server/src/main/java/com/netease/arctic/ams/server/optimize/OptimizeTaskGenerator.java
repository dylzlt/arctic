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

import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.TableTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OptimizeTaskGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizeTaskGenerator.class);

  protected final ArcticTable arcticTable;
  protected final int queueId;
  protected final List<OptimizePlanner> planners;

  public OptimizeTaskGenerator(ArcticTable arcticTable, int queueId) {
    this.arcticTable = arcticTable;
    this.queueId = queueId;
    this.planners = generatePlanners(arcticTable);
  }

  public List<BaseOptimizeTask> generateOptimizeTasks() {
    long startTime = System.currentTimeMillis();
    for (OptimizePlanner optimizePlanner : planners) {
      List<BaseOptimizeTask> optimizeTasks = optimizePlanner.plan();
      if (CollectionUtils.isNotEmpty(optimizeTasks)) {
        LOG.debug("{} plan get {} tasks and total cost {} ms", arcticTable.id(), optimizeTasks.size(),
            System.currentTimeMillis() - startTime);
        return optimizeTasks;
      }
    }

    LOG.debug("{} plan get no tasks, cost {} ms", arcticTable.id(), System.currentTimeMillis() - startTime);
    return Collections.emptyList();
  }

  private List<OptimizePlanner> generatePlanners(ArcticTable arcticTable) {
    if (TableTypeUtil.isIcebergTableFormat(arcticTable)) {
      return generateIcebergPlanners();
    } else if (com.netease.arctic.hive.utils.TableTypeUtil.isHive(arcticTable)) {
      return generateMixedHivePlanners();
    } else {
      return generateMixedIcebergPlanners();
    }
  }

  private List<OptimizePlanner> generateIcebergPlanners() {
    List<OptimizePlanner> icebergOptimizePlanners = new ArrayList<>();
    OptimizePlanner<DataFile, DataFile, DeleteFile, DeleteFile, FileScanTask> fullOptimize =
        new IcebergFullOptimizePlanner();
    OptimizePlanner<DataFile, DataFile, DeleteFile, DeleteFile, FileScanTask> minorOptimize =
        new IcebergMinorOptimizePlanner();
    icebergOptimizePlanners.add(fullOptimize);
    icebergOptimizePlanners.add(minorOptimize);
    Collections.sort(icebergOptimizePlanners);

    return icebergOptimizePlanners;
  }
}
