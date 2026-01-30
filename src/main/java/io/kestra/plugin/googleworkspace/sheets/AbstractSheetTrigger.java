package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSheetTrigger extends AbstractTrigger {

    private static final String APPLICATION_NAME = "Kestra";

    @Schema(
        title = "Service account key",
        description = "Service account JSON with Sheets and Drive access; must cover spreadsheets.readonly and drive.metadata.readonly scopes"
    )
    protected Property<String> serviceAccount;

    @Schema(
        title = "OAuth scopes",
        description = "Scopes applied to the service account credentials; defaults to Sheets readonly and Drive metadata readonly"
    )
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(Arrays.asList(
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly"
    ));

    protected Sheets sheetsConnection(RunContext runContext) throws Exception {
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

        var rScopes = runContext.render(scopes).asList(String.class);
        credentials = credentials.createScoped(rScopes);

        return new Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }

    protected Drive driveConnection(RunContext runContext) throws Exception {
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

        var rScopes = runContext.render(scopes).asList(String.class);
        credentials = credentials.createScoped(rScopes);

        return new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}
