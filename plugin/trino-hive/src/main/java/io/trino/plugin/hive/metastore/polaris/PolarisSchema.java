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

import com.fasterxml.jackson.databind.PropertyNamingStrategies.KebabCaseStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;
import java.util.Map;

/**
 * Represents a table schema from Polaris.
 * This is used for both Iceberg and Generic tables.
 */
@JsonNaming(KebabCaseStrategy.class)
public record PolarisSchema(
        int schemaId,
        List<PolarisField> fields)
{
    /**
     * Creates a schema for Generic tables with a default schema ID.
     */
    public static PolarisSchema forGenericTable(List<PolarisField> fields)
    {
        return new PolarisSchema(1, fields);
    }

    /**
     * Represents a field in a Polaris schema.
     */
    @JsonNaming(KebabCaseStrategy.class)
    public record PolarisField(
            int id,
            String name,
            boolean required,
            String type,
            Map<String, String> metadata)
    {
        /**
         * Creates a field for Generic tables with default ID and nullable conversion.
         */
        public static PolarisField forGenericTable(String name, String type, boolean nullable, Map<String, String> metadata)
        {
            return new PolarisField(0, name, !nullable, type, metadata);
        }
    }
}
