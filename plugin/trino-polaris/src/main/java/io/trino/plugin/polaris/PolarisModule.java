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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergModule;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.TrinoRestCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static java.util.Objects.requireNonNull;

public class PolarisModule
        implements Module
{
    private final CatalogName catalogName;

    public PolarisModule(CatalogName catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PolarisConfig.class);
        binder.install(new IcebergModule());

        // bind the dedicated HttpClient for custom Polaris API calls
        binder.bind(HttpClient.class)
                .annotatedWith(ForPolaris.class)
                .toInstance(new JettyHttpClient(new io.airlift.http.client.HttpClientConfig()));

        // Bind the main TrinoCatalog interface to our decorating implementation
        binder.bind(TrinoCatalog.class).to(PolarisTrinoCatalog.class).in(Scopes.SINGLETON);
    }

    // This method builds the standard TrinoRestCatalog and makes it available
    // for injection with the @ForStandardRest annotation.
    @Provides
    @Singleton
    @ForStandardRest
    public TrinoCatalog provideStandardRestCatalog(PolarisConfig polarisConfig, TypeManager typeManager)
    {
        RESTSessionCatalog icebergCatalog = new RESTSessionCatalog();
        icebergCatalog.setConf(ImmutableMap.of());
        icebergCatalog.initialize(
                catalogName.toString(),
                ImmutableMap.<String, String>builder()
                        .put("uri", polarisConfig.getUri().orElseThrow().toString())
                        .buildOrThrow());

        Cache<Namespace, Namespace> namespaceCache = EvictableCacheBuilder.newBuilder().maximumSize(100).build();
        Cache<TableIdentifier, TableIdentifier> tableCache = EvictableCacheBuilder.newBuilder().maximumSize(10000).build();

        return new TrinoRestCatalog(
                icebergCatalog,
                catalogName,
                NONE,
                ImmutableMap.of(),
                polarisConfig.isNestedNamespaceEnabled(),
                "trino-version-placeholder",
                typeManager,
                false, // unique table location
                polarisConfig.isCaseInsensitiveNameMatching(),
                namespaceCache,
                tableCache);
    }
}
