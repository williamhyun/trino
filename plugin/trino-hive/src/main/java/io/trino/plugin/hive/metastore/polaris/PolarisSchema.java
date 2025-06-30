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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a table schema from Polaris.
 * This is used for both Iceberg and Generic tables.
 */
public class PolarisSchema
{
    private final int schemaId;
    private final List<PolarisField> fields;

    @JsonCreator
    public PolarisSchema(
            @JsonProperty("schema-id") int schemaId,
            @JsonProperty("fields") List<PolarisSchema.PolarisField> fields)
    {
        this.schemaId = schemaId;
        this.fields = fields != null ? ImmutableList.copyOf(fields) : ImmutableList.of();
    }

    // Constructor for Generic tables (type is ignored, using default schema id)
    public PolarisSchema(String type, List<PolarisSchema.PolarisField> fields)
    {
        this(1, fields);
    }

    @JsonProperty("schema-id")
    public int getSchemaId()
    {
        return schemaId;
    }

    @JsonProperty
    public List<PolarisSchema.PolarisField> getFields()
    {
        return fields;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PolarisSchema that = (PolarisSchema) obj;
        return schemaId == that.schemaId &&
                Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaId, fields);
    }

    @Override
    public String toString()
    {
        return "PolarisSchema{" +
                "schemaId=" + schemaId +
                ", fields=" + fields +
                '}';
    }

    /**
     * Represents a field in a Polaris schema.
     */
    public static class PolarisField
    {
        private final int id;
        private final String name;
        private final boolean required;
        private final String type;
        private final Map<String, String> metadata;

        @JsonCreator
        public PolarisField(
                @JsonProperty("id") int id,
                @JsonProperty("name") String name,
                @JsonProperty("required") boolean required,
                @JsonProperty("type") String type,
                @JsonProperty("metadata") Map<String, String> metadata)
        {
            this.id = id;
            this.name = requireNonNull(name, "name is null");
            this.required = required;
            this.type = requireNonNull(type, "type is null");
            this.metadata = metadata != null ? ImmutableMap.copyOf(metadata) : ImmutableMap.of();
        }

        // Constructor for Generic tables (with metadata)
        public PolarisField(String name, String type, boolean nullable, Map<String, String> metadata)
        {
            this(0, name, !nullable, type, metadata);
        }

        @JsonProperty
        public int getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public boolean isRequired()
        {
            return required;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        @JsonProperty
        public Map<String, String> getMetadata()
        {
            return metadata;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PolarisField that = (PolarisField) obj;
            return id == that.id &&
                    required == that.required &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(type, that.type) &&
                    Objects.equals(metadata, that.metadata);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, name, required, type, metadata);
        }

        @Override
        public String toString()
        {
            return "PolarisField{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", required=" + required +
                    ", type='" + type + '\'' +
                    ", metadata=" + metadata +
                    '}';
        }
    }
}
