package io.trino.plugin.polaris;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.NamespaceNotFoundException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;

import static io.trino.plugin.base.util.JsonUtils.jsonFrom;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
    public List<Namespace> listNamespaces(ConnectorSession session, Optional<Namespace> namespace) {
        return standardRestCatalog.listNamespaces(session, namespace);
    }

    @Override
    public org.apache.iceberg.Table loadTable(ConnectorSession session, TableIdentifier tableIdentifier) {
        return standardRestCatalog.loadTable(session, tableIdentifier);
    }

    // TODO implement all other API calls
    // using the polarisHTTPClient, we can choose to make custom Polaris API calls.
}
