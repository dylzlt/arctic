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


  @org.junit.Test
  public void loadTable() {
    String catalog = "local_iceberg";
    String db = "iceberg_db";
    String table = "iceberg_tb";
    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    ArcticCatalog testCatalog = CatalogLoader.load("thrift://127.0.0.1:1260/" + catalog,
        Maps.newHashMap());
    ArcticTable arcticTable = testCatalog.loadTable(tableIdentifier);
    System.out.println(table);
  }
}
