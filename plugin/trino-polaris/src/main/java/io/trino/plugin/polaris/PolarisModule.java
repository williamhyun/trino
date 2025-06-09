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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.plugin.iceberg.IcebergModule;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class PolarisModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PolarisConfig.class);
        binder.install(new IcebergModule());

        // Override the default TrinoCatalogFactory with Polaris-specific factory.
        binder.bind(TrinoCatalogFactory.class)
                .to(PolarisCatalogFactory.class)
                .in(Scopes.SINGLETON);

        binder.install(new S3FileSystemModule());
        binder.install(new GcsFileSystemModule());
        binder.install(new AzureFileSystemModule());
    }
}
