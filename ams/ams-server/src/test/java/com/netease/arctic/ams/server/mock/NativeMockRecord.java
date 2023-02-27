package com.netease.arctic.ams.server.mock;

import com.google.common.collect.Maps;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.table.ArcticTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class NativeMockRecord {
  private static final String CATALOG = "native_iceberg_catalog";
  private static final String DB = "optimize_local";
  private static final String TABLE = "npk_np_animal_lt";
  private static final String warehouse = "/tmp/arctic/warehouse";
  private static final TableIdentifier tableIdentifier = TableIdentifier.of(DB, TABLE);

  public static final Schema TABLE_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "update", Types.StringType.get())
  );
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(TABLE_SCHEMA)
      .identity("name").build();

  protected Catalog icebergCatalog;
  protected Table icebergTable;

  protected static final LocalDateTime ldt =
      LocalDateTime.of(
          LocalDate.of(2022, 1, 1),
          LocalTime.of(0, 0, 0, 0));

  protected HadoopCatalog hadoopCatalog(Map<String, String> catalogProperties) throws IOException {
    HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    hadoopCatalog.initialize(
        CATALOG,
        ImmutableMap.<String, String>builder()
            .putAll(catalogProperties)
            .put(CatalogProperties.WAREHOUSE_LOCATION, warehouse)
            .build());
    return hadoopCatalog;
  }

  @Before
  public void loadTable() throws Exception {
    icebergCatalog = hadoopCatalog(Collections.emptyMap());
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.FORMAT_VERSION, "2");
    icebergTable = icebergCatalog.createTable(tableIdentifier, TABLE_SCHEMA, SPEC, properties);
//    icebergTable = icebergCatalog.loadTable(tableIdentifier);
  }

  @Test
  public void mockData() throws Exception {
    List<DataFile> dataFiles = new ArrayList<>();
    dataFiles.addAll(insertDataFiles(icebergTable, "add", 0));
    insertEqDeleteFiles(icebergTable, "delete", 0);
    insertPosDeleteFiles(icebergTable, dataFiles, 0);
  }

  @Test
  public void delete() {
    icebergCatalog.dropTable(tableIdentifier);
  }

  protected List<DataFile> insertDataFiles(Table icebergTable, String operationId, int taskId) throws IOException {
    List<DataFile> result = new ArrayList<>();
    PartitionKey partitionKey = new PartitionKey(icebergTable.spec(), icebergTable.schema());
    partitionKey.partition(baseRecords(0, 1, icebergTable.schema()).get(0));
    int length = 10;
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema(), icebergTable.spec());
    for (int i = taskId; i < taskId + length * 10; i = i + length) {
      EncryptedOutputFile encryptedOutputFile =
          OutputFileFactory.builderFor(icebergTable, icebergTable.spec().specId(), i)
              .format(FileFormat.PARQUET)
              .operationId(operationId)
              .build().newOutputFile();
      DataWriter<Record> writer = appenderFactory
          .newDataWriter(encryptedOutputFile, FileFormat.PARQUET, partitionKey);
      for (Record record : baseRecords(i, length, icebergTable.schema())) {
        writer.write(record);
      }
      writer.close();
      result.add(writer.toDataFile());
    }

    AppendFiles baseAppend = icebergTable.newAppend();
    result.forEach(baseAppend::appendFile);
    baseAppend.commit();
    return result;
  }

  protected void insertEqDeleteFiles(Table icebergTable, String operationId, int taskId) throws IOException {
    List<DeleteFile> result = new ArrayList<>();
    List<DataFile> updateResult = new ArrayList<>();
    PartitionKey partitionKey = new PartitionKey(icebergTable.spec(), icebergTable.schema());
    partitionKey.partition(baseRecords(0, 1, icebergTable.schema()).get(0));
    int length = 10;
    List<Integer> equalityFieldIds = Lists.newArrayList(icebergTable.schema().findField("id").fieldId());
    Schema eqDeleteRowSchema = icebergTable.schema().select("id");
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(icebergTable.schema(), icebergTable.spec(),
            ArrayUtil.toIntArray(equalityFieldIds), eqDeleteRowSchema, null);
    GenericAppenderFactory updateAfterAppenderFactory =
        new GenericAppenderFactory(icebergTable.schema(), icebergTable.spec());
    for (int i = 0; i < 0 + length * 10; i = i + length) {
      EncryptedOutputFile encryptedOutputFile =
          OutputFileFactory.builderFor(icebergTable, icebergTable.spec().specId(), taskId + i)
              .format(FileFormat.PARQUET)
              .operationId(operationId)
              .build().newOutputFile();
      EqualityDeleteWriter<Record> writer = appenderFactory
          .newEqDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partitionKey);

      EncryptedOutputFile updateEncryptedOutputFile =
          OutputFileFactory.builderFor(icebergTable, icebergTable.spec().specId(), taskId + i)
              .format(FileFormat.PARQUET)
              .operationId("update")
              .build().newOutputFile();
      DataWriter<Record> updateWriter = updateAfterAppenderFactory
          .newDataWriter(updateEncryptedOutputFile, FileFormat.PARQUET, partitionKey);
      List<Record> records = baseRecords(i, length, icebergTable.schema());
      for (int j = 0; j < records.size(); j++) {
        if (j % 2 == 0) {
          writer.delete(records.get(j));
          Record updateRecord = records.get(j).copy();
          updateRecord.setField("update", "update after");
          updateWriter.write(updateRecord);
        }
      }
      writer.close();
      result.add(writer.toDeleteFile());
      updateWriter.close();
      updateResult.add(updateWriter.toDataFile());
    }

    RowDelta rowDelta = icebergTable.newRowDelta();
    result.forEach(rowDelta::addDeletes);
    updateResult.forEach(rowDelta::addRows);
    rowDelta.commit();
  }

  protected void insertPosDeleteFiles(Table icebergTable, List<DataFile> dataFiles, int taskId) throws IOException {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(icebergTable.schema(), icebergTable.spec());
    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(icebergTable, icebergTable.spec().specId(), taskId)
            .build();
    EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();

    OutputFileFactory outputFileFactory1 =
        OutputFileFactory.builderFor(icebergTable, icebergTable.spec().specId(), taskId + 1)
            .build();
    EncryptedOutputFile outputFile1 = outputFileFactory1.newOutputFile();

    PartitionKey partitionKey = new PartitionKey(icebergTable.spec(), icebergTable.schema());
    partitionKey.partition(baseRecords(0, 1, icebergTable.schema()).get(0));

    List<DeleteFile> result = new ArrayList<>();
    PositionDeleteWriter<Record> writer = appenderFactory
        .newPosDeleteWriter(outputFile, FileFormat.PARQUET, partitionKey);
    PositionDeleteWriter<Record> writer1 = appenderFactory
        .newPosDeleteWriter(outputFile1, FileFormat.PARQUET, partitionKey);
    for (int i = 0; i < dataFiles.size(); i++) {
      DataFile dataFile = dataFiles.get(i);
      if (i % 2 == 0) {
        writer.delete(dataFile.path().toString(), 0L);
      } else {
        writer1.delete(dataFile.path().toString(), 1L);
      }
    }
    writer.close();
    result.add(writer.toDeleteFile());
    writer1.close();
    result.add(writer1.toDeleteFile());

    RowDelta rowDelta = icebergTable.newRowDelta();
    result.forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }

  public List<Record> baseRecords(int start, int length, Schema tableSchema) {
    GenericRecord record = GenericRecord.create(tableSchema);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    for (int i = start; i < start + length; i++) {
      builder.add(record.copy(ImmutableMap.of("id", i, "name", "name", "update", "update before")));
    }

    return builder.build();
  }
}
