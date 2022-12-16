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
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.model.TaskConfig;
import com.netease.arctic.ams.server.utils.FilesStatisticsBuilder;
import com.netease.arctic.ams.server.utils.SequenceNumberFetcher;
import com.netease.arctic.ams.server.utils.UnKeyedTableUtil;
import com.netease.arctic.catalog.BaseIcebergCatalog;
import com.netease.arctic.data.IcebergContentFile;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.SerializationUtil;
import com.netease.arctic.utils.TableTypeUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.util.BinPacking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseIcebergOptimizePlanner extends OptimizePlanner<DataFile, DataFile, DeleteFile, DeleteFile, FileScanTask> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseIcebergOptimizePlan.class);

  protected BaseIcebergCatalog.BaseIcebergTable baseIcebergTable;
  protected List<FileScanTask> fileScanTasks;
  protected long latestOptimizeSnapshotId = TableOptimizeRuntime.INVALID_SNAPSHOT_ID;

  public BaseIcebergOptimizePlanner(ArcticTable arcticTable, List<FileScanTask> fileScanTasks, int queueId, int priority) {
    super(arcticTable, fileScanTasks, queueId, priority);
    Preconditions.checkArgument(TableTypeUtil.isIcebergTableFormat(arcticTable),
        "BaseIcebergOptimizePlanner only support for ICEBERG table format");
  }

  @Override
  public boolean tableNeedPlan() {
    long currentSnapshotId = UnKeyedTableUtil.getSnapshotId(baseIcebergTable);
    LOG.debug("{} ==== latestOptimizeSnapshotId={}, currentSnapshotId={}", baseIcebergTable.id(),
        latestOptimizeSnapshotId, currentSnapshotId);
    boolean isChanged = currentSnapshotId != latestOptimizeSnapshotId;
    this.latestOptimizeSnapshotId = currentSnapshotId;
    return isChanged;
  }

  public long getLatestOptimizeSnapshotId() {
    return latestOptimizeSnapshotId;
  }

  protected List<FileScanTask> filterRepeatFileScanTask(Collection<FileScanTask> fileScanTasks) {
    Set<String> dataFilesPath = new HashSet<>();
    List<FileScanTask> finalFileScanTasks = new ArrayList<>();
    for (FileScanTask fileScanTask : fileScanTasks) {
      if (!dataFilesPath.contains(fileScanTask.file().path().toString())) {
        finalFileScanTasks.add(fileScanTask);
        dataFilesPath.add(fileScanTask.file().path().toString());
      }
    }

    return finalFileScanTasks;
  }

  protected List<List<FileScanTask>> binPackFileScanTask(List<FileScanTask> fileScanTasks) {
    long targetFileSize = targetSize();

    Long sum = fileScanTasks.stream()
        .map(fileScanTask -> fileScanTask.file().fileSizeInBytes()).reduce(0L, Long::sum);
    int taskCnt = (int) (sum / targetFileSize) + 1;

    return new BinPacking.ListPacker<FileScanTask>(targetFileSize, taskCnt, true)
        .pack(fileScanTasks, fileScanTask -> fileScanTask.file().fileSizeInBytes());
  }

  protected void generateOptimizeFile(List<FileScanTask> fileScanTasks,
                                      List<DataFile> dataFiles,
                                      List<DeleteFile> eqDeleteFiles,
                                      List<DeleteFile> posDeleteFiles) {
    Set<String> dataFilesPath = new HashSet<>();
    Set<String> deleteFilesPath = new HashSet<>();

    for (FileScanTask fileScanTask : fileScanTasks) {
      // filter repeat data files
      if (!dataFilesPath.contains(fileScanTask.file().path().toString())) {
        dataFiles.add(fileScanTask.file());
        dataFilesPath.add(fileScanTask.file().path().toString());
      }
      for (DeleteFile delete : fileScanTask.deletes()) {
        // filter repeat delete files
        if (!deleteFilesPath.contains(delete.path().toString())) {
          if (delete.content() == FileContent.POSITION_DELETES) {
            posDeleteFiles.add(delete);
          } else {
            eqDeleteFiles.add(delete);
          }
          deleteFilesPath.add(delete.path().toString());
        }
      }
    }
  }

  protected BaseOptimizeTask generateOptimizeTask(List<DataFile> insertFiles,
                                                  List<DataFile> baseFiles,
                                                  List<DeleteFile> eqDeleteFiles,
                                                  List<DeleteFile> posDeleteFiles,
                                                  SequenceNumberFetcher sequenceNumberFetcher,
                                                  TaskConfig taskConfig) {
    // build task
    BaseOptimizeTask optimizeTask = new BaseOptimizeTask();

    List<ByteBuffer> baseFileBytesList =
        baseFiles.stream().map(dataFile -> {
          IcebergContentFile icebergContentFile =
              new IcebergContentFile(dataFile, sequenceNumberFetcher.sequenceNumberOf(dataFile.path().toString()));
          return SerializationUtil.toByteBuffer(icebergContentFile);
        }).collect(Collectors.toList());
    List<ByteBuffer> insertFileBytesList =
        insertFiles.stream().map(dataFile -> {
          IcebergContentFile icebergContentFile =
              new IcebergContentFile(dataFile, sequenceNumberFetcher.sequenceNumberOf(dataFile.path().toString()));
          return SerializationUtil.toByteBuffer(icebergContentFile);
        }).collect(Collectors.toList());
    List<ByteBuffer> eqDeleteFileBytesList =
        eqDeleteFiles.stream().map(deleteFile -> {
          IcebergContentFile icebergContentFile =
              new IcebergContentFile(deleteFile, sequenceNumberFetcher.sequenceNumberOf(deleteFile.path().toString()));
          return SerializationUtil.toByteBuffer(icebergContentFile);
        }).collect(Collectors.toList());
    List<ByteBuffer> posDeleteFileBytesList =
        posDeleteFiles.stream().map(deleteFile -> {
          IcebergContentFile icebergContentFile =
              new IcebergContentFile(deleteFile, sequenceNumberFetcher.sequenceNumberOf(deleteFile.path().toString()));
          return SerializationUtil.toByteBuffer(icebergContentFile);
        }).collect(Collectors.toList());
    optimizeTask.setBaseFiles(baseFileBytesList);
    optimizeTask.setInsertFiles(insertFileBytesList);
    optimizeTask.setDeleteFiles(eqDeleteFileBytesList);
    optimizeTask.setPosDeleteFiles(posDeleteFileBytesList);

    FilesStatisticsBuilder baseFb = new FilesStatisticsBuilder();
    FilesStatisticsBuilder insertFb = new FilesStatisticsBuilder();
    FilesStatisticsBuilder deleteFb = new FilesStatisticsBuilder();
    FilesStatisticsBuilder posDeleteFb = new FilesStatisticsBuilder();
    baseFiles.stream().map(DataFile::fileSizeInBytes)
        .forEach(baseFb::addFile);
    insertFiles.stream().map(DataFile::fileSizeInBytes)
        .forEach(insertFb::addFile);
    eqDeleteFiles.stream().map(DeleteFile::fileSizeInBytes)
        .forEach(deleteFb::addFile);
    posDeleteFiles.stream().map(DeleteFile::fileSizeInBytes)
        .forEach(posDeleteFb::addFile);

    fillOptimizeTask(optimizeTask, taskConfig, baseFb, insertFb, deleteFb, posDeleteFb);
    return optimizeTask;
  }
}
