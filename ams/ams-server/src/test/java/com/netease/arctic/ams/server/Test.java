package com.netease.arctic.ams.server;

import com.google.common.collect.Maps;
import com.netease.arctic.ams.server.service.impl.SupportHiveSyncService;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.PrimaryKeySpec;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.SerializationUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;

public class Test {
  public static String CATALOG = "native-iceberg-benchmark";
  public static String DB = "native_100w_iceberg_1127";
  @org.junit.Test
  public void test() {
    String stock = "stock";
    String customer = "customer";
    String order_line = "order_line";
    TableIdentifier stockTable = TableIdentifier.of(CATALOG, DB, stock);
    TableIdentifier customerTable = TableIdentifier.of(CATALOG, DB, customer);
    TableIdentifier order_lineTable = TableIdentifier.of(CATALOG, DB, order_line);

    modifyProperties(stockTable, TableProperties.OPTIMIZE_QUOTA, "50");
    modifyProperties(customerTable, TableProperties.OPTIMIZE_QUOTA, "40");
    modifyProperties(order_lineTable, TableProperties.OPTIMIZE_QUOTA, "20");
  }

  private void modifyProperties(TableIdentifier tableIdentifier, String key, String vaule) {
    ArcticCatalog testCatalog = CatalogLoader.load("thrift://10.196.98.23:1260/" + CATALOG,
        Maps.newHashMap());
    ArcticTable arcticTable = testCatalog.loadTable(tableIdentifier);
    arcticTable.asUnkeyedTable().updateProperties().set(key, vaule).commit();
  }

  private void modifyProperties() {
    ArcticCatalog testCatalog = CatalogLoader.load("thrift://10.196.98.23:1260/" + CATALOG,
        Maps.newHashMap());
    for (TableIdentifier listTable : testCatalog.listTables(DB)) {
      ArcticTable arcticTable = testCatalog.loadTable(listTable);
      arcticTable.asUnkeyedTable().updateProperties().set(TableProperties.OPTIMIZE_GROUP, "iceberg_optimize_test").commit();
    }
  }


  @org.junit.Test
  public void loadTable() throws Exception {
    String catalog = "arctic_recsys";
    String db = "appnews";
    String table = "ctc_all_publisher_lake";
    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    ArcticCatalog testCatalog = CatalogLoader.load("thrift://10.196.85.29:18312/" + catalog,
        Maps.newHashMap());
    ArcticTable arcticTable = testCatalog.loadTable(tableIdentifier);
    SupportHiveSyncService.SupportHiveSyncTask.syncIcebergToHive(arcticTable, "test");
    System.out.println(table);
  }

  @org.junit.Test
  public void testFiles() {
    Schema TABLE_SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "op_time", Types.TimestampType.withoutZone())
    );
    PartitionSpec SPEC = PartitionSpec.builderFor(TABLE_SCHEMA)
        .day("op_time").build();
    DataFile FILE_A = DataFiles.builder(SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("op_time_day=2022-01-01") // easy way to set partition data for now
        .withRecordCount(2) // needs at least one record or else metrics will filter it out
        .build();

    ByteBuffer b = SerializationUtil.toByteBuffer(FILE_A);
    ContentFile fb = SerializationUtil.toInternalTableFile(b);
    System.out.println(1);
  }
}
