package com.netease.arctic.ams.server.utils;

import com.netease.arctic.AmsClient;
import com.netease.arctic.ams.api.TableMeta;
import com.netease.arctic.ams.server.mapper.TableMetadataMapper;
import com.netease.arctic.ams.server.model.TableMetadata;
import com.netease.arctic.ams.server.service.IJDBCService;
import com.netease.arctic.ams.server.service.ServiceContainer;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.catalog.CatalogLoader;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.KeyedTable;
import com.netease.arctic.table.TableIdentifier;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ModifyChangeTableSequence;
import org.apache.iceberg.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UpdateTool extends IJDBCService {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateTool.class);

  private final AmsClient metastoreClient;

  public UpdateTool() {
    this.metastoreClient = ServiceContainer.getTableMetastoreHandler();
  }

  public void executeAsync() {
    new Thread(() -> {
      LOG.info("start update");
      updateTransactionIdOfAllKeyedTable();
      LOG.info("finish update");
    }, "update-thread").start();
  }

  private void updateTransactionIdOfAllKeyedTable() {
    try (SqlSession sqlSession = getSqlSession(false)) {
      TableMetadataMapper tableMetadataMapper = getMapper(sqlSession, TableMetadataMapper.class);
      List<TableMetadata> tableMetadataList = tableMetadataMapper.listTableMetas();
      for (TableMetadata tableMetadata : tableMetadataList) {
        TableIdentifier tableIdentifier = tableMetadata.getTableIdentifier();
        try {
          if (isKeyedTable(tableMetadata)) {
            if (tableMetadata.getCurrentTxId() > 0) {
              updateTransactionId(loadTable(tableIdentifier), tableMetadata.getCurrentTxId());
            }
            ServiceContainer.getArcticTransactionService().validTable(tableIdentifier.buildTableIdentifier());
          }
        } catch (Throwable t) {
          LOG.error("failed to update transactionId of {}, ignore and continue", tableIdentifier, t);
        }
      }
    }
  }

  private ArcticTable loadTable(TableIdentifier tableIdentifier) {
    ArcticCatalog catalog = CatalogLoader.load(metastoreClient, tableIdentifier.getCatalog());
    return catalog.loadTable(tableIdentifier);
  }

  private boolean isKeyedTable(TableMetadata tableMetadata) {
    TableMeta meta = tableMetadata.buildTableMeta();
    return meta.getKeySpec() != null &&
        meta.getKeySpec().getFields() != null &&
        meta.getKeySpec().getFields().size() > 0;
  }

  private static void updateTransactionId(ArcticTable arcticTable, long transactionId) {
    if (arcticTable.isKeyedTable()) {
      KeyedTable keyedTable = arcticTable.asKeyedTable();
      Snapshot snapshot = keyedTable.changeTable().currentSnapshot();
      long snapshotSequence = 0;
      if (snapshot != null) {
        snapshotSequence = snapshot.sequenceNumber();
      }
      if (transactionId > snapshotSequence) {
        LOG.info("{} try modify change table sequence from {} to {}", keyedTable.id(), snapshotSequence, transactionId);
        ModifyChangeTableSequence modifyTableSequence =
            new ModifyChangeTableSequence(keyedTable.changeTable().name(),
                ((HasTableOperations) keyedTable.changeTable()).operations());
        modifyTableSequence.sequence(transactionId);
        modifyTableSequence.commit();
        LOG.info("{} success modify change table sequence from {} to {}", keyedTable.id(), snapshotSequence,
            transactionId);
      }
    }
  }


}
