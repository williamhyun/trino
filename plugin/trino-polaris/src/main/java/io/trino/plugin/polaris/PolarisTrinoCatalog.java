package io.trino.plugin.polaris;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PolarisTrinoCatalog implements TrinoCatalog {
    private final TrinoCatalog standardRestCatalog;
    private final HttpClient polarisHttpClient;
    private final PolarisConfig config;

    @Inject
    public PolarisTrinoCatalog(
            @ForStandardRest TrinoCatalog standardRestCatalog, // Inject the standard client
            @ForPolaris HttpClient polarisHttpClient,         // Inject the custom Polaris client
            PolarisConfig config) {
        this.standardRestCatalog = requireNonNull(standardRestCatalog, "standardRestCatalog is null");
        this.polarisHttpClient = requireNonNull(polarisHttpClient, "polarisHttpClient is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return standardRestCatalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return standardRestCatalog.loadNamespaceMetadata(session, namespace);
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return standardRestCatalog.listTables(session, namespace);
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName tableName)
    {
        return standardRestCatalog.loadTable(session, tableName);
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        standardRestCatalog.dropNamespace(session, namespace);
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return standardRestCatalog.getNamespacePrincipal(session, namespace);
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        standardRestCatalog.createNamespace(session, namespace, properties, owner);
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, Optional<String> location, Map<String, String> properties)
    {
        return standardRestCatalog.newCreateTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, location, properties);
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return standardRestCatalog.newCreateOrReplaceTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, location, properties);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        standardRestCatalog.dropTable(session, schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        standardRestCatalog.dropCorruptedTable(session, schemaTableName);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        standardRestCatalog.renameTable(session, from, to);
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        standardRestCatalog.updateTableComment(session, schemaTableName, comment);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return standardRestCatalog.defaultTableLocation(session, schemaTableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        standardRestCatalog.setTablePrincipal(session, schemaTableName, principal);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        standardRestCatalog.createView(session, schemaViewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        standardRestCatalog.renameView(session, source, target);
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        standardRestCatalog.setViewPrincipal(session, schemaViewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        standardRestCatalog.dropView(session, schemaViewName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return standardRestCatalog.getViews(session, namespace);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return standardRestCatalog.getView(session, viewName);
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        standardRestCatalog.updateViewComment(session, schemaViewName, comment);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        standardRestCatalog.updateViewColumnComment(session, schemaViewName, columnName, comment);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return standardRestCatalog.tryGetColumnMetadata(session, tables);
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        standardRestCatalog.updateColumnComment(session, schemaTableName, columnIdentity, comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return standardRestCatalog.redirectTable(session, tableName, hiveCatalogName);
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        standardRestCatalog.registerTable(session, tableName, tableMetadata);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        standardRestCatalog.unregisterTable(session, tableName);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> properties, boolean replace, boolean ignoreExisting)
    {
        standardRestCatalog.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        standardRestCatalog.dropMaterializedView(session, viewName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return standardRestCatalog.getMaterializedView(session, viewName);
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return standardRestCatalog.getMaterializedViewStorageTable(session, viewName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return standardRestCatalog.getMaterializedViewProperties(session, viewName, definition);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        standardRestCatalog.renameMaterializedView(session, source, target);
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        standardRestCatalog.updateMaterializedViewColumnComment(session, schemaViewName, columnName, comment);
    }

    // TODO implement all other API calls
    // using the polarisHTTPClient, we can choose to make custom Polaris API calls.
}
