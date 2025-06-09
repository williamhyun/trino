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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogFileSystemFactory;
import io.trino.plugin.iceberg.catalog.rest.TrinoRestCatalog;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PolarisCatalogFactory
        implements TrinoCatalogFactory
{
    private final PolarisConfig config;
    private final IcebergRestCatalogFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final JsonCodec<Map<String, String>> sessionPropertiesCodec;

    @Inject
    public PolarisCatalogFactory(
            PolarisConfig config,
            IcebergRestCatalogFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            JsonCodec<Map<String, String>> sessionPropertiesCodec)
    {
        this.config = requireNonNull(config, "config is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.sessionPropertiesCodec = requireNonNull(sessionPropertiesCodec, "sessionPropertiesCodec is null");
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoRestCatalog(
                config.getUri().orElseThrow(() -> new IllegalStateException("Polaris server URI is not set")),
                Optional.empty(),
                identity,
                fileSystemFactory,
                typeManager,
                sessionPropertiesCodec);
    }
}
