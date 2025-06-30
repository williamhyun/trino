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

import com.fasterxml.jackson.core.type.TypeReference;
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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * REST client for Apache Polaris catalog API.
 * Handles both standard Iceberg REST API and Polaris-specific extensions.
 */
public class PolarisRestClient
{
    private final HttpClient httpClient;
    private final PolarisMetastoreConfig config;
    private final ObjectMapper objectMapper;

    @Inject
    public PolarisRestClient(
            HttpClient httpClient,
            PolarisMetastoreConfig config,
            ObjectMapper objectMapper)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    // Namespace Operations

    public List<PolarisNamespace> listNamespaces(Optional<String> parent)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces")
                .addParameter("parent", parent.orElse(null))
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<List<PolarisNamespace>, RuntimeException>()
        {
            @Override
            public List<PolarisNamespace> handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to list namespaces: " + exception.getMessage(), exception);
            }

            @Override
            public List<PolarisNamespace> handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    JsonNode namespacesNode = root.get("namespaces");

                    ImmutableList.Builder<PolarisNamespace> namespaces = ImmutableList.builder();
                    if (namespacesNode != null && namespacesNode.isArray()) {
                        for (JsonNode namespaceNode : namespacesNode) {
                            String namespaceName = String.join(".",
                                    objectMapper.convertValue(namespaceNode, String[].class));
                            namespaces.add(new PolarisNamespace(namespaceName, ImmutableMap.of()));
                        }
                    }
                    return namespaces.build();
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse namespaces response", e);
                }
            }
        });
    }

    public PolarisNamespace getNamespace(String namespaceName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName))
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<PolarisNamespace, RuntimeException>()
        {
            @Override
            public PolarisNamespace handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to get namespace: " + exception.getMessage(), exception);
            }

            @Override
            public PolarisNamespace handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    JsonNode propertiesNode = root.get("properties");

                    Map<String, String> properties = ImmutableMap.of();
                    if (propertiesNode != null) {
                        properties = objectMapper.convertValue(propertiesNode, new TypeReference<Map<String, String>>() {});
                    }

                    return new PolarisNamespace(namespaceName, properties);
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse namespace response", e);
                }
            }
        });
    }

    public void createNamespace(PolarisNamespace namespace)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces").build();

        Map<String, Object> requestBody = ImmutableMap.of(
                "namespace", namespace.getName().split("\\."),
                "properties", namespace.getProperties());

        Request request = preparePost()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .addHeader("Content-Type", "application/json")
                .setBodyGenerator(createJsonBodyGenerator(requestBody))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to create namespace: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 409) {
                    throw new PolarisAlreadyExistsException("Namespace already exists: " + namespace.getName());
                }
                return null;
            }
        });
    }

    public void dropNamespace(String namespaceName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName))
                .build();

        Request request = prepareDelete()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to drop namespace: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                }
                return null;
            }
        });
    }

    public void updateNamespaceProperties(String namespaceName, Map<String, String> updates, List<String> removals)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/properties")
                .build();

        Map<String, Object> requestBody = ImmutableMap.of(
                "updates", updates,
                "removals", removals);

        Request request = preparePost()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .addHeader("Content-Type", "application/json")
                .setBodyGenerator(createJsonBodyGenerator(requestBody))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to update namespace properties: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                }
                return null;
            }
        });
    }

    // Iceberg Table Operations

    public List<PolarisTableIdentifier> listTables(String namespaceName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables")
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<List<PolarisTableIdentifier>, RuntimeException>()
        {
            @Override
            public List<PolarisTableIdentifier> handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to list tables: " + exception.getMessage(), exception);
            }

            @Override
            public List<PolarisTableIdentifier> handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    JsonNode identifiersNode = root.get("identifiers");

                    ImmutableList.Builder<PolarisTableIdentifier> tables = ImmutableList.builder();
                    if (identifiersNode != null && identifiersNode.isArray()) {
                        for (JsonNode identifierNode : identifiersNode) {
                            JsonNode namespaceNode = identifierNode.get("namespace");
                            JsonNode nameNode = identifierNode.get("name");

                            if (namespaceNode != null && nameNode != null) {
                                String[] namespaceArray = objectMapper.convertValue(namespaceNode, String[].class);
                                String namespace = String.join(".", namespaceArray);
                                String name = nameNode.asText();
                                tables.add(new PolarisTableIdentifier(namespace, name));
                            }
                        }
                    }
                    return tables.build();
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse tables response", e);
                }
            }
        });
    }

    public PolarisTableMetadata loadTable(String namespaceName, String tableName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<PolarisTableMetadata, RuntimeException>()
        {
            @Override
            public PolarisTableMetadata handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to load table: " + exception.getMessage(), exception);
            }

            @Override
            public PolarisTableMetadata handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Table not found: " + namespaceName + "." + tableName);
                    }

                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    return parseTableMetadata(root);
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse table metadata response", e);
                }
            }
        });
    }

    public boolean tableExists(String namespaceName, String tableName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .build();

        Request request = prepareHead()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<Boolean, RuntimeException>()
        {
            @Override
            public Boolean handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to check table existence: " + exception.getMessage(), exception);
            }

            @Override
            public Boolean handle(Request request, io.airlift.http.client.Response response)
            {
                return response.getStatusCode() == 200;
            }
        });
    }

    public void dropTable(String namespaceName, String tableName, boolean purgeRequested)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .addParameter("purgeRequested", String.valueOf(purgeRequested))
                .build();

        Request request = prepareDelete()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to drop table: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Table not found: " + namespaceName + "." + tableName);
                }
                return null;
            }
        });
    }

    public void renameTable(String sourceNamespace, String sourceTable, String destNamespace, String destTable)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/tables/rename").build();

        Map<String, Object> requestBody = ImmutableMap.of(
                "source", ImmutableMap.of(
                        "namespace", sourceNamespace.split("\\."),
                        "name", sourceTable),
                "destination", ImmutableMap.of(
                        "namespace", destNamespace.split("\\."),
                        "name", destTable));

        Request request = preparePost()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .addHeader("Content-Type", "application/json")
                .setBodyGenerator(createJsonBodyGenerator(requestBody))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to rename table: " + exception.getMessage(), exception);
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Table not found: " + sourceNamespace + "." + sourceTable);
                }
                return null;
            }
        });
    }

    // Generic Table Operations

    public List<PolarisTableIdentifier> listGenericTables(String namespaceName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables")
                .build();

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
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .build();

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
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables")
                .build();

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
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .build();

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
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/tables/" + tableName)
                .build();

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

    // View Operations

    public List<PolarisTableIdentifier> listViews(String namespaceName)
    {
        URI uri = buildUri("/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/views")
                .build();

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<List<PolarisTableIdentifier>, RuntimeException>()
        {
            @Override
            public List<PolarisTableIdentifier> handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to list views: " + exception.getMessage(), exception);
            }

            @Override
            public List<PolarisTableIdentifier> handle(Request request, io.airlift.http.client.Response response)
            {
                try {
                    if (response.getStatusCode() == 404) {
                        throw new PolarisNotFoundException("Namespace not found: " + namespaceName);
                    }

                    // Parse views response - similar to tables
                    JsonNode root = objectMapper.readTree(response.getInputStream());
                    JsonNode identifiersNode = root.get("identifiers");

                    ImmutableList.Builder<PolarisTableIdentifier> views = ImmutableList.builder();
                    if (identifiersNode != null && identifiersNode.isArray()) {
                        for (JsonNode identifierNode : identifiersNode) {
                            JsonNode namespaceNode = identifierNode.get("namespace");
                            JsonNode nameNode = identifierNode.get("name");

                            if (namespaceNode != null && nameNode != null) {
                                String[] namespaceArray = objectMapper.convertValue(namespaceNode, String[].class);
                                String namespace = String.join(".", namespaceArray);
                                String name = nameNode.asText();
                                views.add(new PolarisTableIdentifier(namespace, name));
                            }
                        }
                    }
                    return views.build();
                }
                catch (Exception e) {
                    throw new PolarisException("Failed to parse views response", e);
                }
            }
        });
    }

    // Helper methods

    private UriBuilder buildUri(String path)
    {
        return UriBuilder.fromUri(config.getUri()).path(path);
    }

    private String encodeNamespace(String namespace)
    {
        return namespace.replace(".", "%1F"); // Use ASCII Unit Separator for namespace encoding
    }

    private Map<String, String> getAuthHeaders()
    {
        // For now, return empty headers - basic authentication can be added later
        return ImmutableMap.of();
    }

    private Multimap<String, String> buildHeaders(Map<String, String> headers)
    {
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        headers.forEach(builder::put);
        return builder.build();
    }

    private <T> T execute(Request request, ResponseHandler<T, RuntimeException> responseHandler)
    {
        try {
            return httpClient.execute(request, responseHandler);
        }
        catch (Exception e) {
            throw new PolarisException("Request failed: " + e.getMessage(), e);
        }
    }

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

    private PolarisTableMetadata parseTableMetadata(JsonNode root)
    {
        // Parse Iceberg table metadata from JSON response
        JsonNode metadataNode = root.get("metadata");
        if (metadataNode == null) {
            throw new PolarisException("Invalid table metadata response");
        }

        String location = metadataNode.path("location").asText();
        Map<String, Object> schema = objectMapper.convertValue(metadataNode.get("schema"), new TypeReference<Map<String, Object>>() {});
        Map<String, String> properties = objectMapper.convertValue(metadataNode.get("properties"), new TypeReference<Map<String, String>>() {});

        return new PolarisTableMetadata(location, schema, properties);
    }

    private PolarisGenericTable parseGenericTable(JsonNode root)
    {
        // Parse generic table from JSON response
        String name = root.path("name").asText();
        String location = root.path("location").asText();
        Map<String, Object> schemaMap = objectMapper.convertValue(root.get("schema"), new TypeReference<Map<String, Object>>() {});
        Map<String, String> properties = objectMapper.convertValue(root.get("properties"), new TypeReference<Map<String, String>>() {});

        // Convert schema map to PolarisSchema
        PolarisSchema schema = convertSchemaMap(schemaMap);

        return new PolarisGenericTable(name, location, schema, properties);
    }

    private PolarisSchema convertSchemaMap(Map<String, Object> schemaMap)
    {
        if (schemaMap == null) {
            return null;
        }

        String type = (String) schemaMap.get("type");
        Object fieldsObj = schemaMap.get("fields");
        List<Map<String, Object>> fieldsData = objectMapper.convertValue(fieldsObj, new TypeReference<List<Map<String, Object>>>() {});

        List<PolarisSchema.PolarisField> fields = ImmutableList.of();
        if (fieldsData != null) {
            fields = fieldsData.stream()
                    .map(this::convertFieldMap)
                    .collect(toImmutableList());
        }

        return new PolarisSchema(type, fields);
    }

    private PolarisSchema.PolarisField convertFieldMap(Map<String, Object> fieldMap)
    {
        String name = (String) fieldMap.get("name");
        String type = (String) fieldMap.get("type");
        Boolean nullable = (Boolean) fieldMap.getOrDefault("nullable", true);
        Object metadataObj = fieldMap.getOrDefault("metadata", ImmutableMap.of());
        Map<String, String> metadata = objectMapper.convertValue(metadataObj, new TypeReference<Map<String, String>>() {});

        return new PolarisSchema.PolarisField(name, type, nullable, metadata);
    }

    private static class UriBuilder
    {
        private final URI baseUri;
        private final StringBuilder pathBuilder;
        private final StringBuilder queryBuilder;

        private UriBuilder(URI baseUri)
        {
            this.baseUri = requireNonNull(baseUri, "baseUri is null");
            this.pathBuilder = new StringBuilder(baseUri.getPath());
            this.queryBuilder = new StringBuilder();
        }

        public static UriBuilder fromUri(URI uri)
        {
            return new UriBuilder(uri);
        }

        public UriBuilder path(String path)
        {
            if (!path.startsWith("/") && !pathBuilder.toString().endsWith("/")) {
                pathBuilder.append("/");
            }
            pathBuilder.append(path);
            return this;
        }

        public UriBuilder addParameter(String name, String value)
        {
            if (value != null) {
                if (queryBuilder.length() > 0) {
                    queryBuilder.append("&");
                }
                queryBuilder.append(name).append("=").append(value);
            }
            return this;
        }

        public URI build()
        {
            try {
                String query = queryBuilder.length() > 0 ? queryBuilder.toString() : null;
                return new URI(baseUri.getScheme(), baseUri.getAuthority(), pathBuilder.toString(), query, null);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to build URI", e);
            }
        }
    }
}
