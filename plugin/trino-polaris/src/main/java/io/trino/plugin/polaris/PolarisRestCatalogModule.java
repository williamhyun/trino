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
package io.trino.plugin.polaris;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.AwsProperties;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogFileSystemFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogSigV4Config;
import io.trino.plugin.iceberg.catalog.rest.NoneSecurityModule;
import io.trino.plugin.iceberg.catalog.rest.OAuth2SecurityModule;
import io.trino.plugin.iceberg.catalog.rest.SigV4AwsProperties;
import io.trino.plugin.iceberg.catalog.rest.TrinoIcebergRestCatalogFactory;
import io.trino.spi.TrinoException;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class PolarisRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                PolarisConfig.class,
                config -> config.getSecurityType() == PolarisConfig.PolarisSecurityType.OAUTH2,
                new OAuth2SecurityModule(),
                new NoneSecurityModule()));
        install(conditionalModule(
                PolarisConfig.class,
                PolarisConfig::isSigV4Enabled,
                internalBinder -> {
                    internalBinder.bind(AwsProperties.class).to(SigV4AwsProperties.class).in(Scopes.SINGLETON);
                },
                internalBinder -> internalBinder.bind(AwsProperties.class).toInstance(() -> ImmutableMap.of())));

        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        PolarisConfig polarisConfig = buildConfigObject(PolarisConfig.class);
        if (polarisConfig.isVendedCredentialsEnabled() && polarisConfig.isRegisterTableProcedureEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
        }
    }

    @Provides
    @Singleton
    protected IcebergRestCatalogConfig buildRestCatalogConfig(PolarisConfig polarisConfig)
    {
        IcebergRestCatalogConfig restConfig = new IcebergRestCatalogConfig();
        polarisConfig.getUri().ifPresent(uri -> restConfig.setBaseUri(uri.toString()));
        polarisConfig.getWarehouseIdentifier().ifPresent(restConfig::setWarehouse);

        // Map from PolarisConfig enum to the IcebergRestCatalogConfig enum
        restConfig.setSecurity(switch (polarisConfig.getSecurityType()) {
            case NONE -> IcebergRestCatalogConfig.Security.NONE;
            case OAUTH2 -> IcebergRestCatalogConfig.Security.OAUTH2;
        });
        restConfig.setSessionType(switch (polarisConfig.getSessionType()) {
            case NONE -> IcebergRestCatalogConfig.SessionType.NONE;
            case USER -> IcebergRestCatalogConfig.SessionType.USER;
        });

        restConfig.setSessionTimeout(polarisConfig.getSessionTimeout());
        restConfig.setVendedCredentialsEnabled(polarisConfig.isVendedCredentialsEnabled());
        restConfig.setViewEndpointsEnabled(polarisConfig.isViewEndpointsEnabled());
        restConfig.setSigV4Enabled(polarisConfig.isSigV4Enabled());
        restConfig.setCaseInsensitiveNameMatching(polarisConfig.isCaseInsensitiveNameMatching());
        restConfig.setCaseInsensitiveNameMatchingCacheTtl(polarisConfig.getCaseInsensitiveNameMatchingCacheTtl());
        return restConfig;
    }

    @Provides
    @Singleton
    protected IcebergRestCatalogSigV4Config buildRestCatalogSigV4Config(PolarisConfig polarisConfig)
    {
        return new IcebergRestCatalogSigV4Config()
                .setSigningName(polarisConfig.getSigV4SigningName());
    }
}
