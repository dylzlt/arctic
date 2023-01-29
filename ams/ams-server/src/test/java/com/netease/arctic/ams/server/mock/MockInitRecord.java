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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MockInitRecord {
  private static final String CATALOG = "arctic-benchmark";
  private static final String DB = "optimize100w_0129_lt3";
  private static final String TABLE = "stock";
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
    testCatalog = CatalogLoader.load("thrift://10.196.98.23:1260/" + CATALOG,
        Maps.newHashMap());
    arcticTable = testCatalog.loadTable(TABLE_ID);
    rowType = FlinkSchemaUtil.convert(arcticTable.schema());
  }

  // arcticTable.updateProperties().set("self-optimizing.enabled", "true").set("self-optimizing.group", "iceberg_optimize_test").set("self-optimizing.minor.trigger.interval", "60000").set("self-optimizing.major.trigger.interval", "60000").commit();

  @Test
  public void mockData() throws Exception {
    arcticTable.io().doAs(() -> {
//      for (int i = 0; i < 11; i++) {
//        insertBase(arcticTable, 0, 1l, rowType);
//        Thread.sleep(1000);
//      }
//      insertInsert(arcticTable);
//      insetUpdate(arcticTable, 2l, 10000);

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
          RowKind.INSERT, 1, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 2, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 3, TimestampData.fromLocalDateTime(ldt.plusDays(1)), StringData.fromString("john")));
      add(GenericRowData.ofKind(
          RowKind.INSERT, 4, TimestampData.fromLocalDateTime(ldt.plusDays(1)), StringData.fromString("john")));
    }};
    for (RowData record : baseData) {
      taskWriter.write(record);
    }
    commit(arcticTable, taskWriter.complete(), true);
  }

  protected void insertInsert(ArcticTable arcticTable) throws Exception {
    //write change insert
    {
      TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, ChangeLocationKind.INSTANT, 3, 1l, rowType);
      List<RowData> insert = new ArrayList<RowData>() {{
        add(GenericRowData.ofKind(
            RowKind.INSERT, 1, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 2, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 3, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 4, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 5, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 6, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
        add(GenericRowData.ofKind(
            RowKind.INSERT, 7, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john")));
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
      TaskWriter<RowData> taskWriter = createTaskWriter(arcticTable, ChangeLocationKind.INSTANT, 4,  txId, rowType);
      List<RowData> update = new ArrayList<RowData>();
      for (int i = 1; i <= count; i++) {
        RowData before = GenericRowData.ofKind(
            RowKind.UPDATE_BEFORE, i, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john"));
        RowData after = GenericRowData.ofKind(
            RowKind.UPDATE_AFTER, i, TimestampData.fromLocalDateTime(ldt), StringData.fromString("john"));
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
