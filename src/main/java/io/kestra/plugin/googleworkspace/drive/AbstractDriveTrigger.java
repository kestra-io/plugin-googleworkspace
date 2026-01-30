package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractDriveTrigger extends AbstractTrigger {

    private static final String APPLICATION_NAME = "Kestra";
    private static final String DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";

    @Schema(
        title = "Service account key",
        description = "Google service account JSON with Drive scope; if omitted, Application Default Credentials are used"
    )
    protected Property<String> serviceAccount;

    public Drive from(RunContext runContext) throws Exception {
        GoogleCredentials credentials;

        if (this.serviceAccount != null) {
            String rServiceAccount = runContext.render(serviceAccount)
                .as(String.class)
                .orElseThrow();

            credentials = GoogleCredentials.fromStream(
                new ByteArrayInputStream(rServiceAccount.getBytes(StandardCharsets.UTF_8))
            );
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        credentials = credentials.createScoped(Collections.singleton(DRIVE_SCOPE));

        return new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}
