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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.avro.AvroFileWriterFactory;
import io.trino.plugin.hive.avro.AvroPageSourceFactory;
import io.trino.plugin.hive.esri.EsriPageSourceFactory;
import io.trino.plugin.hive.fs.CachingDirectoryLister;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryListerFactory;
import io.trino.plugin.hive.line.CsvFileWriterFactory;
import io.trino.plugin.hive.line.CsvPageSourceFactory;
import io.trino.plugin.hive.line.JsonFileWriterFactory;
import io.trino.plugin.hive.line.JsonPageSourceFactory;
import io.trino.plugin.hive.line.OpenXJsonFileWriterFactory;
import io.trino.plugin.hive.line.OpenXJsonPageSourceFactory;
import io.trino.plugin.hive.line.RegexFileWriterFactory;
import io.trino.plugin.hive.line.RegexPageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFileWriterFactory;
import io.trino.plugin.hive.line.SimpleTextFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleTextFileWriterFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(HiveMetastoreConfig.class);
        configBinder(binder).bindConfig(SortingFileWriterConfig.class, "hive");

        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveViewProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveColumnProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveAnalyzeProperties.class).in(Scopes.SINGLETON);

        binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);

        binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).withGeneratedName();
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        Multibinder<SystemTableProvider> systemTableProviders = newSetBinder(binder, SystemTableProvider.class);
        systemTableProviders.addBinding().to(PartitionsSystemTableProvider.class).in(Scopes.SINGLETON);
        systemTableProviders.addBinding().to(PropertiesSystemTableProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TransactionalMetadataFactory.class)
                .setDefault().to(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(TransactionScopeCachingDirectoryListerFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generator -> generator.generatedNameOf(HiveSplitManager.class));
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(CsvPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(JsonPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(OpenXJsonPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(EsriPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RegexPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(SimpleTextFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(SimpleSequenceFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(AvroPageSourceFactory.class).in(Scopes.SINGLETON);

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).withGeneratedName();
        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(CsvFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(JsonFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RegexFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(OpenXJsonFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(SimpleTextFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(SimpleSequenceFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(AvroFileWriterFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(ParquetFileWriterFactory.class).in(Scopes.SINGLETON);

        binder.install(new HiveExecutorModule());
    }

    @Provides
    @Singleton
    @HideDeltaLakeTables
    public boolean hideDeltaLakeTables(HiveMetastoreConfig config)
    {
        return config.isHideDeltaLakeTables();
    }
}
