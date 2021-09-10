package io.kestra.plugin.googleworkspace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task implements GcpInterface {
    protected static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    protected String projectId;

    protected String serviceAccount;

    protected HttpCredentialsAdapter credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials;

        if (serviceAccount != null) {
            String serviceAccount = runContext.render(this.serviceAccount);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccount.getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
            Logger logger = runContext.logger();

            if (logger.isTraceEnabled()) {
                byteArrayInputStream.reset();
                Map<String, String> jsonKey = JacksonMapper.ofJson().readValue(byteArrayInputStream, new TypeReference<>() {});
                if (jsonKey.containsKey("client_email")) {
                    logger.trace(" • Using service account: {}", jsonKey.get("client_email") );
                }
            }
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        if (this.getScopes() != null) {
            credentials = credentials.createScoped(runContext.render(this.getScopes()));
        }

        return new HttpCredentialsAdapter(credentials);
    }

    protected NetHttpTransport netHttpTransport() throws GeneralSecurityException, IOException {
        return GoogleNetHttpTransport.newTrustedTransport();
    }
}
