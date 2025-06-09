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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PolarisConfig
{
    public enum PolarisSecurityType
    {
        NONE,
        OAUTH2,
    }

    public enum PolarisSessionType
    {
        NONE,
        USER
    }

    // IcebergRestCatalogConfig properties
    private URI uri;
    private Optional<String> restPrefix = Optional.empty();
    private Optional<String> warehouseIdentifier = Optional.empty();
    private boolean nestedNamespaceEnabled;
    private PolarisSecurityType securityType = PolarisSecurityType.NONE;
    private PolarisSessionType sessionType = PolarisSessionType.NONE;
    private Duration sessionTimeout = new Duration(CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT, MILLISECONDS);
    private boolean vendedCredentialsEnabled;
    private boolean viewEndpointsEnabled = true;
    private boolean sigV4Enabled;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);

    // IcebergRestCatalogSigV4Config properties
    private String sigV4SigningName = AwsProperties.REST_SIGNING_NAME_DEFAULT;

    // OAuth2SecurityConfig properties
    private String oauth2Credential;
    private String oauth2Scope;
    private String oauth2Token;
    private URI oauth2ServerUri;
    private boolean oauth2TokenRefreshEnabled = OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT;

    // IcebergConfig (general iceberg behavior) properties to be added here
    private boolean registerTableProcedureEnabled;

    @NotNull
    public Optional<URI> getUri()
    {
        return Optional.ofNullable(this.uri);
    }

    @Config("polaris.uri")
    @ConfigDescription("The URI to the Polaris REST server")
    public PolarisConfig setUri(String uri)
    {
        if (uri != null) {
            this.uri = URI.create(uri);
        }
        return this;
    }

    public Optional<String> getRestPrefix()
    {
        return restPrefix;
    }

    @Config("polaris.rest.prefix")
    @ConfigDescription("The prefix for the resource path to use with the Polaris REST catalog server")
    public PolarisConfig setRestPrefix(String prefix)
    {
        this.restPrefix = Optional.ofNullable(prefix);
        return this;
    }

    public Optional<String> getWarehouseIdentifier()
    {
        return warehouseIdentifier;
    }

    @Config("polaris.warehouse")
    @ConfigDescription("The warehouse location/identifier to use with the Polaris REST catalog server")
    public PolarisConfig setWarehouseIdentifier(String warehouse)
    {
        this.warehouseIdentifier = Optional.ofNullable(warehouse);
        return this;
    }

    public boolean isNestedNamespaceEnabled()
    {
        return nestedNamespaceEnabled;
    }

    @Config("polaris.nested-namespace-enabled")
    @ConfigDescription("Support querying objects under nested namespace")
    public PolarisConfig setNestedNamespaceEnabled(boolean nestedNamespaceEnabled)
    {
        this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        return this;
    }

    @NotNull
    public PolarisSecurityType getSecurityType()
    {
        return securityType;
    }

    @Config("polaris.security.type")
    @ConfigDescription("Authorization protocol to use when communicating with the Polaris REST catalog server (e.g., NONE, OAUTH2)")
    public PolarisConfig setSecurityType(PolarisSecurityType security)
    {
        this.securityType = security;
        return this;
    }

    @NotNull
    public PolarisSessionType getSessionType()
    {
        return sessionType;
    }

    @Config("polaris.session.type")
    @ConfigDescription("Type of REST catalog session to use when communicating with Polaris REST catalog Server (e.g., NONE, USER)")
    public PolarisConfig setSessionType(PolarisSessionType sessionType)
    {
        this.sessionType = sessionType;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getSessionTimeout()
    {
        return sessionTimeout;
    }

    @Config("polaris.rest.session.timeout")
    @ConfigDescription("Duration to keep authentication session in cache for REST communication")
    public PolarisConfig setSessionTimeout(Duration sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public boolean isVendedCredentialsEnabled()
    {
        return vendedCredentialsEnabled;
    }

    @Config("polaris.rest.vended-credentials.enabled")
    @ConfigDescription("Use credentials provided by the Polaris REST backend for file system access")
    public PolarisConfig setVendedCredentialsEnabled(boolean vendedCredentialsEnabled)
    {
        this.vendedCredentialsEnabled = vendedCredentialsEnabled;
        return this;
    }

    public boolean isViewEndpointsEnabled()
    {
        return viewEndpointsEnabled;
    }

    @Config("polaris.rest.view-endpoints.enabled")
    @ConfigDescription("Enable view endpoints for REST communication")
    public PolarisConfig setViewEndpointsEnabled(boolean viewEndpointsEnabled)
    {
        this.viewEndpointsEnabled = viewEndpointsEnabled;
        return this;
    }

    public boolean isSigV4Enabled()
    {
        return sigV4Enabled;
    }

    @Config("polaris.security.sigv4.enabled")
    @ConfigDescription("Enable AWS Signature version 4 (SigV4) for REST communication")
    public PolarisConfig setSigV4Enabled(boolean sigV4Enabled)
    {
        this.sigV4Enabled = sigV4Enabled;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("polaris.rest.case-insensitive-name-matching.enabled")
    @ConfigDescription("Match object names case-insensitively via the REST catalog")
    public PolarisConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("polaris.rest.case-insensitive-name-matching.cache-ttl")
    @ConfigDescription("Duration to keep case insensitive object mapping prior to eviction for REST catalog")
    public PolarisConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    @NotNull
    public String getSigV4SigningName()
    {
        return sigV4SigningName;
    }

    @Config("polaris.security.sigv4.signing-name")
    @ConfigDescription("AWS SigV4 signing service name for Polaris REST API")
    public PolarisConfig setSigV4SigningName(String signingName)
    {
        this.sigV4SigningName = signingName;
        return this;
    }

    public Optional<String> getOauth2Credential()
    {
        return Optional.ofNullable(oauth2Credential);
    }

    @Config("polaris.security.oauth2.credential")
    @ConfigDescription("The credential to exchange for a token in the OAuth2 client credentials flow (e.g., client_id:client_secret)")
    @ConfigSecuritySensitive
    public PolarisConfig setOauth2Credential(String credential)
    {
        this.oauth2Credential = credential;
        return this;
    }

    public Optional<String> getOauth2Scope()
    {
        return Optional.ofNullable(oauth2Scope);
    }

    @Config("polaris.security.oauth2.scope")
    @ConfigDescription("The scope which will be used for OAuth2 interactions with the server")
    public PolarisConfig setOauth2Scope(String scope)
    {
        this.oauth2Scope = scope;
        return this;
    }

    public Optional<String> getOauth2Token()
    {
        return Optional.ofNullable(oauth2Token);
    }

    @Config("polaris.security.oauth2.token")
    @ConfigDescription("The Bearer token which will be used for OAuth2 interactions with the server")
    @ConfigSecuritySensitive
    public PolarisConfig setOauth2Token(String token)
    {
        this.oauth2Token = token;
        return this;
    }

    public Optional<URI> getOauth2ServerUri()
    {
        return Optional.ofNullable(oauth2ServerUri);
    }

    @Config("polaris.security.oauth2.server-uri")
    @ConfigDescription("The endpoint to retrieve access token from OAuth2 Server")
    public PolarisConfig setOauth2ServerUri(URI serverUri)
    {
        this.oauth2ServerUri = serverUri;
        return this;
    }

    public boolean isOauth2TokenRefreshEnabled()
    {
        return oauth2TokenRefreshEnabled;
    }

    @Config("polaris.security.oauth2.token-refresh.enabled")
    @ConfigDescription("Controls whether an OAuth2 token should be refreshed if its expiration information is available")
    public PolarisConfig setOauth2TokenRefreshEnabled(boolean tokenRefreshEnabled)
    {
        this.oauth2TokenRefreshEnabled = tokenRefreshEnabled;
        return this;
    }

    @AssertTrue(message = "OAuth2 requires a credential or token if security type is OAUTH2")
    public boolean isOauth2CredentialOrTokenPresentIfEnabled()
    {
        if (securityType == PolarisSecurityType.OAUTH2) {
            return oauth2Credential != null || oauth2Token != null;
        }
        return true;
    }

    @AssertTrue(message = "OAuth2 scope is applicable only when using credential, not with a pre-provided token")
    public boolean isOauth2ScopePresentOnlyWithCredential()
    {
        return !(oauth2Token != null && oauth2Scope != null);
    }

    // --- Placeholder for general Iceberg behavioral settings ---

    public boolean isRegisterTableProcedureEnabled()
    {
        return registerTableProcedureEnabled;
    }

    @Config("polaris.iceberg.register-table-procedure.enabled") // Example
    @ConfigDescription("Allow users to call the register_table procedure")
    public PolarisConfig setRegisterTableProcedureEnabled(boolean registerTableProcedureEnabled)
    {
        this.registerTableProcedureEnabled = registerTableProcedureEnabled;
        return this;
    }

    // Add other getters and setters from IcebergConfig as required
}
