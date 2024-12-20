package io.kestra.plugin.googleworkspace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task implements GcpInterface {
    protected static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    protected Property<String> serviceAccount;

    @Builder.Default
    protected Property<Integer> readTimeout = Property.of(120);


    protected HttpCredentialsAdapter credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials;

        if (serviceAccount != null) {
            String serviceAccount = runContext.render(this.serviceAccount).as(String.class).orElseThrow();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccount.getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
            Logger logger = runContext.logger();

            if (logger.isTraceEnabled()) {
                byteArrayInputStream.reset();
                Map<String, String> jsonKey = JacksonMapper.ofJson().readValue(byteArrayInputStream, new TypeReference<>() {});
                if (jsonKey.containsKey("client_email")) {
                    logger.trace(" • Using service account: {}", jsonKey.get("client_email"));
                }
            }
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        var renderedScopes = runContext.render(this.getScopes()).asList(String.class);
        if (!renderedScopes.isEmpty()) {
            credentials = credentials.createScoped(renderedScopes);
        }

        var renderedTiemout = runContext.render(this.readTimeout).as(Integer.class).orElseThrow();
        return new HttpCredentialsAdapter(credentials) {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                super.initialize(request);

                request.setReadTimeout(renderedTiemout * 1000);
            }
        };
    }

    protected NetHttpTransport netHttpTransport() throws GeneralSecurityException, IOException {
        return GoogleNetHttpTransport.newTrustedTransport();
    }
}
