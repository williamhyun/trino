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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * REST client for Apache Polaris catalog API.
 *
 * This client follows the TrinoRestCatalog pattern:
 * - Delegates standard Iceberg operations to RESTSessionCatalog
 * - Uses direct HttpClient for Polaris-specific Generic Table operations
 * - Provides unified interface for both Iceberg and Delta Lake table operations
 */
public class PolarisRestClient
{
    private final RESTSessionCatalog restSessionCatalog;
    private final HttpClient httpClient;
    private final PolarisMetastoreConfig config;
    private final ObjectMapper objectMapper;

    @Inject
    public PolarisRestClient(
            RESTSessionCatalog restSessionCatalog,
            @ForPolarisClient HttpClient httpClient,
            PolarisMetastoreConfig config,
            ObjectMapper objectMapper)
    {
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    // ICEBERG OPERATIONS (via RESTSessionCatalog)

    /**
     * Lists Iceberg tables using the standard REST catalog
     */
    public List<PolarisTableIdentifier> listIcebergTables(String namespaceName)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace namespace = Namespace.of(namespaceName.split("\\."));

            return restSessionCatalog.listTables(sessionContext, namespace).stream()
                    .map(id -> new PolarisTableIdentifier(id.namespace().toString(), id.name()))
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to list Iceberg tables: " + e.getMessage(), e);
        }
    }

    /**
     * Loads Iceberg table metadata using the standard REST catalog
     */
    public PolarisTableMetadata loadIcebergTable(String namespaceName, String tableName)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

            org.apache.iceberg.Table table = restSessionCatalog.loadTable(sessionContext, tableId);
            return convertIcebergTableToPolaris(table);
        }
        catch (NoSuchTableException e) {
            throw new PolarisNotFoundException("Iceberg table not found: " + namespaceName + "." + tableName);
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to load Iceberg table: " + e.getMessage(), e);
        }
    }

    /**
     * Creates Iceberg table using the standard REST catalog
     */
    public PolarisTableMetadata createIcebergTable(String namespaceName, String tableName, Map<String, Object> tableRequest)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

            // Extract schema and other properties from tableRequest
            org.apache.iceberg.Schema schema = extractSchemaFromRequest(tableRequest);
            org.apache.iceberg.PartitionSpec partitionSpec = extractPartitionSpecFromRequest(tableRequest);
            Map<String, String> properties = extractPropertiesFromRequest(tableRequest);

            org.apache.iceberg.Table table = restSessionCatalog.buildTable(sessionContext, tableId, schema)
                    .withPartitionSpec(partitionSpec)
                    .withProperties(properties)
                    .create();

            return convertIcebergTableToPolaris(table);
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to create Iceberg table: " + e.getMessage(), e);
        }
    }

    /**
     * Renames Iceberg table using the standard REST catalog
     */
    public void renameIcebergTable(String sourceNamespace, String sourceTable, String destNamespace, String destTable)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier sourceId = TableIdentifier.of(sourceNamespace, sourceTable);
            TableIdentifier destId = TableIdentifier.of(destNamespace, destTable);

            restSessionCatalog.renameTable(sessionContext, sourceId, destId);
        }
        catch (NoSuchTableException e) {
            throw new PolarisNotFoundException("Iceberg table not found: " + sourceNamespace + "." + sourceTable);
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to rename Iceberg table: " + e.getMessage(), e);
        }
    }

    /**
     * Drops Iceberg table using the standard REST catalog
     */
    public void dropIcebergTable(String namespaceName, String tableName, boolean purgeRequested)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

            if (purgeRequested) {
                restSessionCatalog.purgeTable(sessionContext, tableId);
            }
            else {
                restSessionCatalog.dropTable(sessionContext, tableId);
            }
        }
        catch (NoSuchTableException e) {
            throw new PolarisNotFoundException("Iceberg table not found: " + namespaceName + "." + tableName);
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to drop Iceberg table: " + e.getMessage(), e);
        }
    }

    // NAMESPACE OPERATIONS (via RESTSessionCatalog)

    /**
     * Lists namespaces using the standard REST catalog
     */
    public List<PolarisNamespace> listNamespaces(Optional<String> parent)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace parentNamespace = parent.map(p -> Namespace.of(p.split("\\."))).orElse(Namespace.empty());

            return restSessionCatalog.listNamespaces(sessionContext, parentNamespace).stream()
                    .map(ns -> new PolarisNamespace(ns.toString(), ImmutableMap.of()))
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to list namespaces: " + e.getMessage(), e);
        }
    }

    /**
     * Creates namespace using the standard REST catalog
     */
    public void createNamespace(PolarisNamespace namespace)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace ns = Namespace.of(namespace.getName().split("\\."));

            restSessionCatalog.createNamespace(sessionContext, ns, namespace.getProperties());
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to create namespace: " + e.getMessage(), e);
        }
    }

    // GENERIC TABLE OPERATIONS (via HttpClient)

    /**
     * Lists Generic (Delta Lake) tables using Polaris-specific API
     */
    public List<PolarisTableIdentifier> listGenericTables(String namespaceName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables");

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<List<PolarisTableIdentifier>, RuntimeException>()
        {
            @Override
            public List<PolarisTableIdentifier> handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to list generic tables: " + exception.getMessage(), exception);
            }

            @Override
            public List<PolarisTableIdentifier> handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    JsonNode tablesNode = root.get("tables");

                    ImmutableList.Builder<PolarisTableIdentifier> tables = ImmutableList.builder();
                    if (tablesNode != null && tablesNode.isArray()) {
                        for (JsonNode tableNode : tablesNode) {
                            JsonNode nameNode = tableNode.get("name");
                            if (nameNode != null) {
                                tables.add(new PolarisTableIdentifier(namespaceName, nameNode.asText()));
                            }
                        }
                    }
                    return tables.build();
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse generic tables response", e);
                }
            }
        });
    }

    public PolarisGenericTable loadGenericTable(String namespaceName, String tableName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName);

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<PolarisGenericTable, RuntimeException>()
        {
            @Override
            public PolarisGenericTable handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to load generic table: " + exception.getMessage(), exception);
            }

            @Override
            public PolarisGenericTable handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Generic table not found: " + namespaceName + "." + tableName);
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    return parseGenericTable(root);
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse generic table response", e);
                }
            }
        });
    }

    public PolarisGenericTable createGenericTable(String namespaceName, PolarisGenericTable table)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables");

        Request request = preparePost()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .addHeader("Content-Type", "application/json")
                .setBodyGenerator(createJsonBodyGenerator(table))
                .build();

        return execute(request, new ResponseHandler<PolarisGenericTable, RuntimeException>()
        {
            @Override
            public PolarisGenericTable handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to create generic table: " + exception.getMessage(), exception);
            }

            @Override
            public PolarisGenericTable handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 409) {
                        throw new PolarisAlreadyExistsException("Generic table already exists: " + namespaceName + "." + table.getName());
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    return parseGenericTable(root);
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse create generic table response", e);
                }
            }
        });
    }

    public boolean genericTableExists(String namespaceName, String tableName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName);

        Request request = prepareHead()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<Boolean, RuntimeException>()
        {
            @Override
            public Boolean handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to check generic table existence: " + exception.getMessage(), exception);
            }

            @Override
            public Boolean handle(Request request, io.airlift.http.client.Response response)
            {
                return response.getStatusCode() == 200;
            }
        });
    }

    public void dropGenericTable(String namespaceName, String tableName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName);

        Request request = prepareDelete()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to drop generic table: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Generic table not found: " + namespaceName + "." + tableName);
                }
                return null;
            }
        });
    }

    // HELPER METHODS

    /**
     * Creates session context for Iceberg operations
     */
    private SessionCatalog.SessionContext createSessionContext()
    {
        String sessionId = UUID.randomUUID().toString();
        Map<String, String> credentials = getAuthHeaders();
        Map<String, String> properties = ImmutableMap.of(
                "catalog", config.getPrefix(),
                "warehouse", config.getUri().toString());

        return new SessionCatalog.SessionContext(sessionId, "polaris-user", credentials, properties, null);
    }

    /**
     * Converts Iceberg Table to PolarisTableMetadata
     */
    private PolarisTableMetadata convertIcebergTableToPolaris(org.apache.iceberg.Table table)
    {
        // Implementation to convert Iceberg table metadata to Polaris format
        // This would extract schema, properties, etc. from the Iceberg table
        throw new UnsupportedOperationException("Implementation needed");
    }

    // AUTHENTICATION & HTTP UTILITIES

    /**
     * Gets authentication headers based on configured auth type
     */
    private Map<String, String> getAuthHeaders()
    {
        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();

        switch (config.getAuthType().toUpperCase()) {
            case "BEARER_TOKEN":
                config.getToken().ifPresent(token ->
                            headers.put("Authorization", "Bearer " + token));
                break;
            case "OAUTH2":
                // For OAuth2, we would typically get the token from RESTSessionCatalog
                // and reuse it for Generic Table operations
                config.getToken().ifPresent(token ->
                            headers.put("Authorization", "Bearer " + token));
                break;
            case "NONE":
            default:
                // No authentication headers
                break;
        }

        // Add any extra headers from config
        config.getExtraHeaders().ifPresent(headers::putAll);

        return headers.buildOrThrow();
    }

    /**
     * Builds headers for HTTP requests
     */
    private Multimap<String, String> buildHeaders(Map<String, String> headers)
    {
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        headers.forEach(builder::put);
        return builder.build();
    }

    /**
     * Executes HTTP request with error handling
     */
    private <T> T execute(Request request, ResponseHandler<T, RuntimeException> responseHandler)
    {
        try {
            return httpClient.execute(request, responseHandler);
        }
        catch (Exception e) {
            throw new PolarisException("Request failed: " + e.getMessage(), e);
        }
    }

    /**
     * Creates JSON body generator for HTTP requests
     */
    private BodyGenerator createJsonBodyGenerator(Object object)
    {
        try {
            String json = objectMapper.writeValueAsString(object);
            return StaticBodyGenerator.createStaticBodyGenerator(json, UTF_8);
        }
        catch (Exception e) {
            throw new PolarisException("Failed to serialize request body", e);
        }
    }

    /**
     * Builds URI for API requests by simply concatenating base URI with path
     */
    private URI buildUri(String path)
    {
        String baseUri = config.getUri().toString();
        if (baseUri.endsWith("/") && path.startsWith("/")) {
            return URI.create(baseUri + path.substring(1));
        }
        if (!baseUri.endsWith("/") && !path.startsWith("/")) {
            return URI.create(baseUri + "/" + path);
        }
        return URI.create(baseUri + path);
    }

    /**
     * Encodes namespace for URL path
     */
    private String encodeNamespace(String namespace)
    {
        return namespace.replace(".", "%1F");
    }

    // TODO: CONVERSION METHODS

    private org.apache.iceberg.Schema extractSchemaFromRequest(Map<String, Object> tableRequest)
    {
        throw new UnsupportedOperationException("Schema extraction not implemented yet");
    }

    private org.apache.iceberg.PartitionSpec extractPartitionSpecFromRequest(Map<String, Object> tableRequest)
    {
        throw new UnsupportedOperationException("Partition spec extraction not implemented yet");
    }

    private Map<String, String> extractPropertiesFromRequest(Map<String, Object> tableRequest)
    {
        throw new UnsupportedOperationException("Properties extraction not implemented yet");
    }

    private PolarisGenericTable parseGenericTable(JsonNode root)
    {
        throw new UnsupportedOperationException("Generic table parsing not implemented yet");
    }
}
