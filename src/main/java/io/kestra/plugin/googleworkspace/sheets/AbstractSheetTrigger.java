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
        title = "The Google Cloud service account key",
        description = "Service account JSON key with access to Google Sheets API and Drive API. " +
            "The service account must have spreadsheets.readonly and drive.metadata.readonly scopes."
    )
    @NotNull
    protected Property<String> serviceAccount;

    @Schema(
        title = "Google API OAuth scopes",
        description = "List of OAuth scopes required for the API calls"
    )
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(Arrays.asList(
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly"
    ));

    protected Sheets sheetsConnection(RunContext runContext) throws Exception {
        String rServiceAccount = runContext.render(serviceAccount).as(String.class).orElseThrow();
        var rScopes = runContext.render(scopes).asList(String.class);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(rServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(rScopes);

        return new Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }

    protected Drive driveConnection(RunContext runContext) throws Exception {
        String rServiceAccount = runContext.render(serviceAccount).as(String.class).orElseThrow();
        var rScopes = runContext.render(scopes).asList(String.class);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(rServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(rScopes);

        return new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}
