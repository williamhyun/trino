/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.polaris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class PolarisHiveMetastore
        implements HiveMetastore
{
    private final PolarisRestClient polarisClient;

    @Inject
    public PolarisHiveMetastore(PolarisRestClient polarisClient)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            PolarisNamespace namespace = polarisClient.getNamespace(databaseName);
            return Optional.of(Database.builder()
                    .setDatabaseName(namespace.getName())
                    .setParameters(namespace.getProperties())
                    .build());
        }
        catch (PolarisNotFoundException e) {
            return Optional.empty();
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get database: " + databaseName, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return polarisClient.listNamespaces(Optional.empty())
                    .stream()
                    .map(PolarisNamespace::getName)
                    .collect(toImmutableList());
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list databases", e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        // Try Iceberg tables first
        try {
            if (polarisClient.tableExists(databaseName, tableName)) {
                PolarisTableMetadata metadata = polarisClient.loadTable(databaseName, tableName);
                return Optional.of(convertIcebergToHiveTable(databaseName, tableName, metadata));
            }
        }
        catch (PolarisNotFoundException ignored) {
            // Fall through to try generic tables
        }

        // Try generic tables
        try {
            if (polarisClient.genericTableExists(databaseName, tableName)) {
                PolarisGenericTable genericTable = polarisClient.loadGenericTable(databaseName, tableName);
                return Optional.of(convertGenericToHiveTable(databaseName, tableName, genericTable));
            }
        }
        catch (PolarisNotFoundException ignored) {
            // Table not found
        }

        return Optional.empty();
    }

    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ImmutableSet.of();
    }

    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return PartitionStatistics.empty();
    }

    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        throw new UnsupportedOperationException("Table statistics updates are not supported by Polaris");
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        throw new UnsupportedOperationException("Partition statistics updates are not supported by Polaris");
    }

    public List<String> getAllTables(String databaseName)
    {
        try {
            // Get both Iceberg and generic tables
            List<String> icebergTables = polarisClient.listTables(databaseName)
                    .stream()
                    .map(PolarisTableIdentifier::getName)
                    .collect(toImmutableList());

            List<String> genericTables = polarisClient.listGenericTables(databaseName)
                    .stream()
                    .map(PolarisTableIdentifier::getName)
                    .collect(toImmutableList());

            return ImmutableList.<String>builder()
                    .addAll(icebergTables)
                    .addAll(genericTables)
                    .build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list tables in database: " + databaseName, e);
        }
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return getAllTables(databaseName).stream()
                .map(tableName -> new TableInfo(new SchemaTableName(databaseName, tableName), TableInfo.ExtendedRelationType.TABLE))
                .collect(toImmutableList());
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return ImmutableList.of();
    }

    public List<String> getAllViews(String databaseName)
    {
        try {
            return polarisClient.listViews(databaseName)
                    .stream()
                    .map(PolarisTableIdentifier::getName)
                    .collect(toImmutableList());
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list views in database: " + databaseName, e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            PolarisNamespace namespace = new PolarisNamespace(database.getDatabaseName(), database.getParameters());
            polarisClient.createNamespace(namespace);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create database: " + database.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            polarisClient.dropNamespace(databaseName);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop database: " + databaseName, e);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Rename database is not supported by Polaris");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Database ownership is not supported by Polaris");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table creation is not yet supported");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        // Try to drop from both Iceberg and generic table APIs
        try {
            if (polarisClient.tableExists(databaseName, tableName)) {
                polarisClient.dropTable(databaseName, tableName, deleteData);
                return;
            }
        }
        catch (RuntimeException e) {
            // Fall through to try generic tables
        }

        try {
            if (polarisClient.genericTableExists(databaseName, tableName)) {
                polarisClient.dropGenericTable(databaseName, tableName);
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop table: " + databaseName + "." + tableName, e);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        throw new TrinoException(NOT_SUPPORTED, "Replace table is not supported by Polaris");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            polarisClient.renameTable(databaseName, tableName, newDatabaseName, newTableName);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to rename table", e);
        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table comments are not supported by Polaris");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table ownership is not supported by Polaris");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column comments are not supported by Polaris");
    }

    // Partition operations - handled by format-specific connectors
    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    // Role and privilege operations - not supported by Polaris
    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return ImmutableSet.of();
    }

    // Transaction operations - not supported by Polaris
    @Override
    public Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        // No-op for Polaris
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    // Function operations - not supported by Polaris
    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return ImmutableList.of();
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return ImmutableList.of();
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    public void alterFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    // Column statistics operations - not supported yet
    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    // Schema modification operations - not supported
    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Add column is not supported by Polaris");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Rename column is not supported by Polaris");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Drop column is not supported by Polaris");
    }

    // Privilege operations - not supported
    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("Table privileges are not supported by Polaris");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("Table privileges are not supported by Polaris");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    // Helper methods for converting Polaris tables to Hive tables
    private Table convertIcebergToHiveTable(String databaseName, String tableName, PolarisTableMetadata metadata)
    {
        return Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(ImmutableList.of()) // Schema will be handled by Iceberg connector
                .setParameters(ImmutableMap.<String, String>builder()
                        .putAll(metadata.getProperties())
                        .put("table_type", "ICEBERG")
                        .put("metadata_location", metadata.getLocation())
                        .build())
                .withStorage(storage -> storage
                        .setLocation(metadata.getLocation())
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.mapred.FileInputFormat",
                                "org.apache.hadoop.mapred.FileOutputFormat",
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
                        .setSerdeParameters(ImmutableMap.of("path", metadata.getLocation())))
                .build();
    }

    private Table convertGenericToHiveTable(String databaseName, String tableName, PolarisGenericTable genericTable)
    {
        List<Column> columns = ImmutableList.of();
        if (genericTable.getSchema() != null) {
            columns = genericTable.getSchema().fields().stream()
                    .map(field -> new Column(field.name(), HiveType.valueOf(field.type()), Optional.empty(), ImmutableMap.of()))
                    .collect(toImmutableList());
        }

        return Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(columns)
                .setParameters(ImmutableMap.<String, String>builder()
                        .putAll(genericTable.getProperties())
                        .put("spark.sql.sources.provider", "DELTA")
                        .build())
                .withStorage(storage -> storage
                        .setLocation(genericTable.getLocation())
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.mapred.FileInputFormat",
                                "org.apache.hadoop.mapred.FileOutputFormat",
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
                        .setSerdeParameters(ImmutableMap.of("path", genericTable.getLocation())))
                .build();
    }
}
