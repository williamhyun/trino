/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.TrinoRestCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static java.util.Objects.requireNonNull;

public class PolarisCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final PolarisConfig polarisConfig;
    private final JsonCodec<Map<String, String>> sessionPropertiesCodec;

    @Inject
    public PolarisCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            PolarisConfig polarisConfig,
            JsonCodec<Map<String, String>> sessionPropertiesCodec)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.polarisConfig = requireNonNull(polarisConfig, "polarisConfig is null");
        this.sessionPropertiesCodec = requireNonNull(sessionPropertiesCodec, "sessionPropertiesCodec is null");
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        // Step 1: Create the underlying Apache Iceberg RESTSessionCatalog
        RESTSessionCatalog icebergCatalog = new RESTSessionCatalog();

        // Step 2: Configure it with properties for Polaris
        icebergCatalog.setConf(ImmutableMap.of()); // No Hadoop conf needed for REST
        icebergCatalog.initialize(
                catalogName.toString(),
                ImmutableMap.<String, String>builder()
                        .put("uri", polarisConfig.getPolarisServerUri().toString())
                        // Add any other properties needed for Polaris REST auth here
                        // For example: .put("token", "...")
                        .buildOrThrow());

        // Step 3: Wrap the configured Iceberg catalog in Trino's implementation
        return new TrinoRestCatalog(
                icebergCatalog,
                catalogName,
                NONE, // Assuming no special session type is needed
                ImmutableMap.of(),
                false,
                "polaris-", // Prefix for table properties
                typeManager,
                false, // unique-table-location
                false, // use-schema-name-in-table-location
                PropertyUtil.propertyAsBoolean(icebergCatalog.properties(), "cache-enabled", true) ?
                        new MapTableIdentifierCache(100) : null,
                PropertyUtil.propertyAsBoolean(icebergCatalog.properties(), "cache-enabled", true) ?
                        new MapTableIdentifierCache(10000) : null);
    }
}
