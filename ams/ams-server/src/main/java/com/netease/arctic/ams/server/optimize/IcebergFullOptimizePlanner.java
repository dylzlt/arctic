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

import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.TaskConfig;
import com.netease.arctic.ams.server.utils.SequenceNumberFetcher;
import com.netease.arctic.table.ArcticTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergFullOptimizePlanner extends BaseIcebergOptimizePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullOptimizePlanner.class);

  protected final Map<String, List<FileScanTask>> partitionFileList = new LinkedHashMap<>();

  public IcebergFullOptimizePlanner(ArcticTable arcticTable,
                                    List<FileScanTask> fileScanTasks,
                                    int queueId, int priority) {
    super(arcticTable, fileScanTasks, queueId, priority);
  }

  @Override
  public boolean filesNeedPlan() {
    LOG.debug("{} start plan iceberg table files", tableId());
    AtomicInteger addCnt = new AtomicInteger();
    for (FileScanTask fileScanTask : fileScanTasks) {
      DataFile dataFile = fileScanTask.file();
      String partitionPath = arcticTable.spec().partitionToPath(dataFile.partition());
      currentPartitions.add(partitionPath);
      List<FileScanTask> fileScanTasks = partitionFileList.computeIfAbsent(partitionPath, p -> new ArrayList<>());
      fileScanTasks.add(fileScanTask);
      addCnt.getAndIncrement();
    }

    LOG.debug("{} ==== {} add {} fileScanTasks" + " After added, partition cnt : {}",
        tableId(), optimizeType(), addCnt, partitionFileList.size());

    return !partitionFileList.isEmpty();
  }

  @Override
  boolean partitionNeedPlan(String partitionToPath) {
    List<FileScanTask> partitionFileScanTasks = partitionFileList.get(partitionToPath);
    if (CollectionUtils.isEmpty(partitionFileScanTasks)) {
      LOG.debug("{} ==== don't need {} optimize plan, skip partition {}, there are no data files",
          tableId(), optimizeType(), partitionToPath);
      return false;
    }

    Set<DeleteFile> partitionDeleteFiles = new HashSet<>();
    for (FileScanTask partitionFileScanTask : partitionFileScanTasks) {
      partitionDeleteFiles.addAll(partitionFileScanTask.deletes());
    }

    double deleteFilesTotalSize = partitionDeleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();

    // delete files total size reach target_size * duplicate_ratio
    if (deleteFilesTotalSize > targetSize() * duplicateRatio()) {
      LOG.debug("{} ==== need native Full optimize plan, partition is {}, " +
              "delete files totalSize is {}, target size is {}, duplicate ratio is {}",
          tableId(), partitionToPath, deleteFilesTotalSize, targetSize(), duplicateRatio());
      return true;
    }

    LOG.debug("{} ==== don't need {} optimize plan, skip partition {}, " +
            "delete files totalSize is {}, target size is {}, duplicate ratio is {}",
        tableId(), optimizeType(), partitionToPath, deleteFilesTotalSize, targetSize(), duplicateRatio());
    return false;
  }

  @Override
  List<BaseOptimizeTask> collectTask(String partitionToPath) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    String commitGroup = UUID.randomUUID().toString();
    long createTime = System.currentTimeMillis();

    TaskConfig taskPartitionConfig = new TaskConfig(partitionToPath, null, null,
        commitGroup, planGroup, optimizeType(), createTime, "");

    List<FileScanTask> fileScanTasks = partitionFileList.get(partitionToPath);
    fileScanTasks = filterRepeatFileScanTask(fileScanTasks);
    List<List<FileScanTask>> binPackFileScanTasks = binPackFileScanTask(fileScanTasks);

    if (CollectionUtils.isNotEmpty(binPackFileScanTasks)) {
      SequenceNumberFetcher sequenceNumberFetcher = new SequenceNumberFetcher(
          arcticTable.asUnkeyedTable(), latestOptimizeSnapshotId);
      for (List<FileScanTask> fileScanTask : binPackFileScanTasks) {
        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> eqDeleteFiles = new ArrayList<>();
        List<DeleteFile> posDeleteFiles = new ArrayList<>();
        generateOptimizeFile(fileScanTask, dataFiles, eqDeleteFiles, posDeleteFiles);

        collector.add(generateOptimizeTask(Collections.emptyList(), dataFiles,
            eqDeleteFiles, posDeleteFiles, sequenceNumberFetcher, taskPartitionConfig));
      }
    }

    return collector;
  }

  @Override
  public OptimizeType optimizeType() {
    return OptimizeType.FullMajor;
  }
}
