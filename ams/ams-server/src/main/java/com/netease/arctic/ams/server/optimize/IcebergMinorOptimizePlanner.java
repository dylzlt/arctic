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
import org.apache.iceberg.StructLike;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IcebergMinorOptimizePlanner extends BaseIcebergOptimizePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergMinorOptimizePlanner.class);

  // partition -> FileScanTask
  private final Map<String, List<FileScanTask>> partitionSmallDataFilesTask = new HashMap<>();
  private final Map<String, List<FileScanTask>> partitionBigDataFilesTask = new HashMap<>();
  // partition -> need optimize delete files
  private final Map<String, Set<DeleteFile>> partitionDeleteFiles = new HashMap<>();
  // delete file path -> related data files
  private final Map<String, Set<FileScanTask>> deleteDataFileMap = new HashMap<>();

  public IcebergMinorOptimizePlanner(ArcticTable arcticTable, List<FileScanTask> fileScanTasks, int queueId, int priority) {
    super(arcticTable, fileScanTasks, queueId, priority);
  }

  @Override
  boolean filesNeedPlan() {
    long smallFileSize = smallFileSize();
    AtomicInteger addBigDataFileScanCnt = new AtomicInteger();
    AtomicInteger addSmallDataFileScanCnt = new AtomicInteger();
    Set<String> addPartition = new HashSet<>();
    for (FileScanTask fileScanTask : fileScanTasks) {
      StructLike partition = fileScanTask.file().partition();
      String partitionPath = arcticTable.asUnkeyedTable().spec().partitionToPath(partition);
      currentPartitions.add(partitionPath);
      // add DataFile info
      if (fileScanTask.file().fileSizeInBytes() <= smallFileSize) {
        // collect small data file info
        List<FileScanTask> smallFileScanTasks =
            partitionSmallDataFilesTask.computeIfAbsent(partitionPath, c -> new ArrayList<>());
        smallFileScanTasks.add(fileScanTask);
        addSmallDataFileScanCnt.getAndIncrement();
        addPartition.add(partitionPath);
      } else {
        // collect need optimize delete file info
        if (fileScanTask.deletes().size() > 1) {
          List<FileScanTask> bigFileScanTasks =
              partitionBigDataFilesTask.computeIfAbsent(partitionPath, c -> new ArrayList<>());
          bigFileScanTasks.add(fileScanTask);
          addBigDataFileScanCnt.getAndIncrement();
          addPartition.add(partitionPath);
        }

        // add DeleteFile info
        for (DeleteFile deleteFile : fileScanTask.deletes()) {
          if (fileScanTask.deletes().size() > 1) {
            Set<DeleteFile> deleteFiles =
                partitionDeleteFiles.computeIfAbsent(partitionPath, c -> new HashSet<>());
            deleteFiles.add(deleteFile);
          }

          String deletePath = deleteFile.path().toString();
          Set<FileScanTask> fileScanTasks =
              deleteDataFileMap.computeIfAbsent(deletePath, c -> new HashSet<>());
          fileScanTasks.add(fileScanTask);
        }
      }
    }

    LOG.debug("{} ==== {} add {} bigDataFilesTasks and {} smallDataFilesTasks" + " After added, partition cnt : {}",
        tableId(), optimizeType(), addBigDataFileScanCnt, addSmallDataFileScanCnt, addPartition.size());

    return !partitionBigDataFilesTask.isEmpty() || !partitionSmallDataFilesTask.isEmpty();
  }

  @Override
  boolean partitionNeedPlan(String partitionToPath) {
    List<FileScanTask> smallDataFileTask = partitionSmallDataFilesTask.getOrDefault(partitionToPath, new ArrayList<>());

    Set<DeleteFile> smallDeleteFile = partitionDeleteFiles.getOrDefault(partitionToPath, new HashSet<>()).stream()
        .filter(deleteFile -> deleteFile.fileSizeInBytes() <= smallFileSize())
        .collect(Collectors.toSet());
    for (FileScanTask task : smallDataFileTask) {
      smallDeleteFile.addAll(task.deletes());
    }

    // partition has greater than 12 small files to optimize(include data files and delete files)
    long smallFileCount = smallDataFileTask.size() + smallDeleteFile.size();
    if (smallFileCount >= smallFileCountThreshold()) {
      LOG.debug("{} ==== need iceberg minor optimize plan, partition is {}, " +
              "small file count is {}, small DataFile count is {}, small DeleteFile count is {}",
          tableId(), partitionToPath, smallFileCount, smallDataFileTask.size(), smallDeleteFile.size());
      return true;
    }

    LOG.debug("{} ==== don't need {} optimize plan, skip partition {}", tableId(), optimizeType(), partitionToPath);
    return false;
  }

  @Override
  List<BaseOptimizeTask> collectTask(String partitionToPath) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    String commitGroup = UUID.randomUUID().toString();
    long createTime = System.currentTimeMillis();

    TaskConfig taskPartitionConfig = new TaskConfig(partitionToPath, null, null,
        commitGroup, planGroup, OptimizeType.Minor, createTime, "");

    SequenceNumberFetcher sequenceNumberFetcher = new SequenceNumberFetcher(
        arcticTable.asUnkeyedTable(), latestOptimizeSnapshotId);
    collector.addAll(collectSmallDataFileTask(partitionToPath, taskPartitionConfig, sequenceNumberFetcher));
    collector.addAll(collectDeleteFileTask(partitionToPath, taskPartitionConfig, sequenceNumberFetcher));

    return collector;
  }

  @Override
  public OptimizeType optimizeType() {
    return OptimizeType.Minor;
  }

  private List<BaseOptimizeTask> collectSmallDataFileTask(String partition,
                                                          TaskConfig taskPartitionConfig,
                                                          SequenceNumberFetcher sequenceNumberFetcher) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    List<FileScanTask> smallFileScanTasks = partitionSmallDataFilesTask.get(partition);
    if (CollectionUtils.isEmpty(smallFileScanTasks)) {
      return collector;
    }

    smallFileScanTasks = filterRepeatFileScanTask(smallFileScanTasks);
    List<List<FileScanTask>> packedList = binPackFileScanTask(smallFileScanTasks);

    if (CollectionUtils.isNotEmpty(packedList)) {
      for (List<FileScanTask> fileScanTasks : packedList) {
        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> eqDeleteFiles = new ArrayList<>();
        List<DeleteFile> posDeleteFiles = new ArrayList<>();
        generateOptimizeFile(fileScanTasks, dataFiles, eqDeleteFiles, posDeleteFiles);

        // only return tasks with at least 2 files
        int totalFileCnt = dataFiles.size() + eqDeleteFiles.size() + posDeleteFiles.size();
        if (totalFileCnt > 1) {
          collector.add(generateOptimizeTask(dataFiles, Collections.emptyList(),
              eqDeleteFiles, posDeleteFiles, sequenceNumberFetcher, taskPartitionConfig));
        }
      }
    }

    return collector;
  }

  private List<BaseOptimizeTask> collectDeleteFileTask(String partition,
                                                       TaskConfig taskPartitionConfig,
                                                       SequenceNumberFetcher sequenceNumberFetcher) {
    List<BaseOptimizeTask> collector = new ArrayList<>();
    Set<DeleteFile> needOptimizeDeleteFiles = partitionDeleteFiles.get(partition);
    if (CollectionUtils.isEmpty(needOptimizeDeleteFiles)) {
      return collector;
    }

    List<FileScanTask> allNeedOptimizeTask = new ArrayList<>();
    for (DeleteFile needOptimizeDeleteFile : needOptimizeDeleteFiles) {
      allNeedOptimizeTask.addAll(deleteDataFileMap.get(needOptimizeDeleteFile.path().toString()));
    }

    allNeedOptimizeTask = filterRepeatFileScanTask(allNeedOptimizeTask);
    List<List<FileScanTask>> packedList = binPackFileScanTask(allNeedOptimizeTask);

    if (CollectionUtils.isNotEmpty(packedList)) {
      for (List<FileScanTask> fileScanTasks : packedList) {
        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> eqDeleteFiles = new ArrayList<>();
        List<DeleteFile> posDeleteFiles = new ArrayList<>();
        generateOptimizeFile(fileScanTasks, dataFiles, eqDeleteFiles, posDeleteFiles);

        // only return tasks with at least 2 files
        int totalFileCnt = dataFiles.size() + eqDeleteFiles.size() + posDeleteFiles.size();
        if (totalFileCnt > 1) {
          collector.add(generateOptimizeTask(Collections.emptyList(), dataFiles,
              eqDeleteFiles, posDeleteFiles, sequenceNumberFetcher, taskPartitionConfig));
        }
      }
    }

    return collector;
  }
}
