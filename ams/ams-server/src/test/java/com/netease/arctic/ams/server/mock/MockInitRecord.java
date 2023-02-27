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

package com.netease.arctic.ams.server.mock;

import com.google.common.collect.Maps;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.flink.write.FlinkTaskWriterBuilder;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.BaseLocationKind;
import com.netease.arctic.table.ChangeLocationKind;
import com.netease.arctic.table.LocationKind;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.table.UnkeyedTable;
import com.netease.arctic.utils.ManifestEntryFields;
import com.netease.arctic.utils.TableFileUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MockInitRecord {
  private static final String CATALOG = "mix_iceberg_catalog_test";
  private static final String DB = "op_smoke";
  private static final String TABLE = "pk_np_animal";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(CATALOG, DB, TABLE);

  protected static final LocalDateTime ldt =
      LocalDateTime.of(
          LocalDate.of(2022, 1, 1),
          LocalTime.of(0, 0, 0, 0));

  protected ArcticCatalog testCatalog;
  protected ArcticTable arcticTable;
  protected RowType rowType;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void loadTable() {
    testCatalog = CatalogLoader.load("thrift://10.196.98.26:18312/" + CATALOG,
        Maps.newHashMap());
    arcticTable = testCatalog.loadTable(TABLE_ID);
    rowType = FlinkSchemaUtil.convert(arcticTable.schema());
  }

  // arcticTable.updateProperties().set("self-optimizing.enabled", "true").set("self-optimizing.group", "iceberg_optimize_test").set("self-optimizing.minor.trigger.interval", "60000").commit();

  private static String metadataTableName(String tableName, MetadataTableType type) {
    return tableName + (tableName.contains("/") ? "#" : ".") + type;
  }

  private Iterable<CloseableIterable<StructLike>> entriesOfManifest(CloseableIterable<FileScanTask> fileScanTasks) {
    return Iterables.transform(fileScanTasks, task -> {
      assert task != null;
      return ((DataTask) task).rows();
    });
  }

  @Test
  public void mockData() throws Exception {
    Set<String> validFilesPath = new HashSet<>();
    UnkeyedTable innerTable = arcticTable.asKeyedTable().changeTable();

    Table manifestTable =
        MetadataTableUtils.createMetadataTableInstance(((HasTableOperations) innerTable).operations(),
            innerTable.name(), metadataTableName(innerTable.name(), MetadataTableType.ALL_ENTRIES),
            MetadataTableType.ALL_ENTRIES);
    try (CloseableIterable<Record> entries = IcebergGenerics.read(manifestTable)
        .select(ManifestEntryFields.STATUS.name(), ManifestEntryFields.DATA_FILE_FIELD_NAME).build()) {
      for (Record entry : entries) {
        ManifestEntryFields.Status status =
            ManifestEntryFields.Status.of((int) entry.get(ManifestEntryFields.STATUS.fieldId()));
        if (status == ManifestEntryFields.Status.ADDED || status == ManifestEntryFields.Status.EXISTING) {
          GenericRecord dataFile = (GenericRecord) entry.get(ManifestEntryFields.DATA_FILE_ID);
          String filePath = (String) dataFile.getField(DataFile.FILE_PATH.name());
          validFilesPath.add(TableFileUtils.getUriPath(filePath));
        }
      }
    } catch (IOException e) {
    }

    System.out.println(validFilesPath + "," + validFilesPath.size());
    arcticTable.io().doAs(() -> {
//      for (int i = 0; i < 11; i++) {
//        insertBase(arcticTable, 0, null, rowType);
//        Thread.sleep(1000);
//      }
//      insertInsert(arcticTable);
//      insetUpdate(arcticTable, null, 20);

//      insetUpdate(7);
      return null;
    });

//    ((SupportHive) arcticTable).getHMSClient().run(client -> {
//      Table table = client.getTable(arcticTable.id().getDatabase(),
//          arcticTable.id().getTableName());
////      table.getSd().setLocation("hdfs://hz11-trino-arctic-0.jd.163.org:8020/user/warehouse/optimize_smoke.db/pk_np_animal_lt/hive/1661950335453_077bbfdb-2c83-44b2-97aa-bad94c36a1b4");
////      client.alter_table(arcticTable.id().getDatabase(),
////          arcticTable.id().getTableName(), table);
//      return null;
//    });
  }

  protected void insertBase(ArcticTable arcticTable, int mask, Long transaction, RowType rowType) throws Exception {
    TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, BaseLocationKind.INSTANT, mask, transaction, rowType);
    List<RowData> baseData = new ArrayList<RowData>() {{
      add(GenericRowData.ofKind(
          RowKind.INSERT, 13, StringData.fromString("john")));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 2, StringData.fromString("john")));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 3, StringData.fromString("john")));
//      add(GenericRowData.ofKind(
//          RowKind.INSERT, 4, StringData.fromString("john")));
    }};
    for (RowData record : baseData) {
      taskWriter.write(record);
    }
    commit(arcticTable, taskWriter.complete(), true);
  }

  protected void insertInsert(ArcticTable arcticTable) throws Exception {
    //write change insert
    {
      TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, ChangeLocationKind.INSTANT, 3, null, rowType);
//      List<RowData> insert = new ArrayList<RowData>() {{
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 1, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 2, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 3, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 4, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 5, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 6, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//        add(GenericRowData.ofKind(
//            RowKind.INSERT, 7, StringData.fromString("2022-01-01"), StringData.fromString("john")));
//      }};
      List<RowData> insert = new ArrayList<RowData>() {{
      add(GenericRowData.ofKind(
          RowKind.INSERT, 101, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 102, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 103, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 104, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 105, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 106, StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 107, StringData.fromString("john")));
    }};
      for (RowData record : insert) {
        taskWriter.write(record);
      }
      commit(arcticTable, taskWriter.complete(), false);
    }
  }

  protected void insetUpdate(ArcticTable arcticTable, Long txId, int count) throws Exception {
    //write change delete
    {
      TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, ChangeLocationKind.INSTANT, 3,  txId, rowType);
      List<RowData> update = new ArrayList<RowData>();
      for (int i = 1; i <= count; i++) {
        RowData before = GenericRowData.ofKind(
            RowKind.UPDATE_BEFORE, i, StringData.fromString("john"));
        RowData after = GenericRowData.ofKind(
            RowKind.UPDATE_AFTER, i, StringData.fromString("john"));
        update.add(before);
        update.add(after);
      }

      for (RowData record : update) {
        taskWriter.write(record);
      }
      commit(arcticTable, taskWriter.complete(), false);
    }
  }

  protected void commit(ArcticTable arcticTable, WriteResult result, boolean base) {
    if (arcticTable.isKeyedTable()) {
      if (base) {
        AppendFiles baseAppend = arcticTable.asKeyedTable().baseTable().newAppend();
        Arrays.stream(result.dataFiles()).forEach(baseAppend::appendFile);
        baseAppend.commit();
      } else {
        AppendFiles changeAppend = arcticTable.asKeyedTable().changeTable().newAppend();
        Arrays.stream(result.dataFiles()).forEach(changeAppend::appendFile);
        changeAppend.commit();
      }
    } else {
      AppendFiles baseAppend = arcticTable.asUnkeyedTable().newAppend();
      Arrays.stream(result.dataFiles()).forEach(baseAppend::appendFile);
      baseAppend.commit();
    }
  }

  protected TaskWriter<RowData> createTaskWriter(ArcticTable arcticTable,
                                                 LocationKind locationKind,
                                                 int mask,
                                                 Long transactionId,
                                                 RowType rowType) {
    if (arcticTable.isKeyedTable()) {
      return FlinkTaskWriterBuilder.buildFor(arcticTable)
          .withTransactionId(transactionId)
          .withMask(mask)
          .withFlinkSchema(rowType)
          .withTaskId(System.currentTimeMillis())
          .buildWriter(locationKind);
    } else {
      return FlinkTaskWriterBuilder.buildFor(arcticTable)
          .withFlinkSchema(rowType)
          .withTransactionId(transactionId)
          .withTaskId(System.currentTimeMillis())
          .buildWriter(locationKind);
    }
  }
}
