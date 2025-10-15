package io.kestra.plugin.googleworkspace.mail;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.OAuthInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractMail extends Task implements OAuthInterface {
    protected static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    @Schema(
        title = "OAuth 2.0 Client ID",
        description = "The OAuth 2.0 client ID from Google Cloud Console"
    )
    @PluginProperty(dynamic = true)
    protected Property<String> clientId;

    @Schema(
        title = "OAuth 2.0 Client Secret",
        description = "The OAuth 2.0 client secret from Google Cloud Console"
    )
    @PluginProperty(dynamic = true)
    protected Property<String> clientSecret;

    @Schema(
        title = "OAuth 2.0 Refresh Token",
        description = "The OAuth 2.0 refresh token obtained through the authorization flow"
    )
    @PluginProperty(dynamic = true)
    protected Property<String> refreshToken;

    @Schema(
        title = "OAuth 2.0 Access Token", 
        description = "The OAuth 2.0 access token (optional, will be generated from refresh token if not provided)"
    )
    @PluginProperty(dynamic = true)
    protected Property<String> accessToken;

    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of(
        GmailScopes.GMAIL_MODIFY,
        GmailScopes.GMAIL_READONLY,
        GmailScopes.GMAIL_SEND
    ));

    @Builder.Default
    protected Property<Integer> readTimeout = Property.ofValue(120);

    protected Gmail connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException, GeneralSecurityException {
        HttpCredentialsAdapter credentials = this.oauthCredentials(runContext);
        
        return new Gmail.Builder(this.netHttpTransport(), JSON_FACTORY, credentials)
            .setApplicationName("Kestra")
            .build();
    }

    protected HttpCredentialsAdapter oauthCredentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        // Get OAuth parameters
        String clientId = runContext.render(this.clientId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientId is required for OAuth authentication"));
        String clientSecret = runContext.render(this.clientSecret).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientSecret is required for OAuth authentication"));
        String refreshToken = runContext.render(this.refreshToken).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("refreshToken is required for OAuth authentication"));
        
        // Optional access token
        String accessToken = runContext.render(this.accessToken).as(String.class).orElse(null);
        
        // Create UserCredentials for OAuth
        UserCredentials.Builder credentialsBuilder = UserCredentials.newBuilder()
            .setClientId(clientId)
            .setClientSecret(clientSecret)
            .setRefreshToken(refreshToken);
        
        if (accessToken != null && !accessToken.trim().isEmpty()) {
            credentialsBuilder.setAccessToken(new AccessToken(accessToken, null));
        }
        
        GoogleCredentials credentials = credentialsBuilder.build();
        
        // Apply scopes if specified
        var renderedScopes = runContext.render(this.scopes).asList(String.class);
        if (renderedScopes != null && !renderedScopes.isEmpty()) {
            credentials = credentials.createScoped(renderedScopes);
        }
        
        var renderedTimeout = runContext.render(this.readTimeout).as(Integer.class).orElse(120);
        return new HttpCredentialsAdapter(credentials) {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                super.initialize(request);
                request.setReadTimeout(renderedTimeout * 1000);
            }
        };
    }
    
    protected NetHttpTransport netHttpTransport() throws GeneralSecurityException, IOException {
        return GoogleNetHttpTransport.newTrustedTransport();
    }
}