package com.netease.arctic.ams.server;

import com.google.common.collect.Maps;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;

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
  public void loadTable() {
    String catalog = "local_catalog";
    String db = "iceberg_db";
    String table = "iceberg_tb";
    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    ArcticCatalog testCatalog = CatalogLoader.load("thrift://127.0.0.1:1260/" + catalog,
        Maps.newHashMap());
    ArcticTable arcticTable = testCatalog.loadTable(tableIdentifier);
    System.out.println(table);
  }
}
