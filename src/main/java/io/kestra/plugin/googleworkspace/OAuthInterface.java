package io.kestra.plugin.googleworkspace;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public interface OAuthInterface {
    @Schema(
        title = "OAuth 2.0 Client ID",
        description = "The OAuth 2.0 client ID from Google Cloud Console"
    )
    Property<String> getClientId();

    @Schema(
        title = "OAuth 2.0 Client Secret", 
        description = "The OAuth 2.0 client secret from Google Cloud Console"
    )
    Property<String> getClientSecret();

    @Schema(
        title = "OAuth 2.0 Refresh Token",
        description = "The OAuth 2.0 refresh token obtained through the authorization flow"
    )
    Property<String> getRefreshToken();

    @Schema(
        title = "OAuth 2.0 Access Token",
        description = "The OAuth 2.0 access token (optional, will be generated from refresh token if not provided)"
    )
    Property<String> getAccessToken();

    @Schema(
        title = "The OAuth scopes to use",
        description = "List of OAuth 2.0 scopes required for the operation"
    )
    Property<List<String>> getScopes();

    @Schema(
        title = "The read timeout for the request (in seconds)"
    )
    Property<Integer> getReadTimeout();
}